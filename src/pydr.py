#%%
import pdal
import dask.dataframe as dd
import dask.array as da
import dask_geopandas
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
import pyarrow as pa
import pyarrow.parquet as pq
from dask import delayed, compute
from dask.diagnostics import ProgressBar
import os
import shutil
from pathlib import Path
import json


#%%

class lasCatalogue:
    '''docstring'''

    def __init__(self, data_dir, tilesize=500):
        self.data_dir = data_dir
        self.parquet_dir = os.path.join(
            os.path.dirname(self.data_dir),
            'parquets')

        self.tilesize = tilesize
        self.files = []
        self.__see_what_files(self.data_dir)
        self.lazy = []
        
        for las in self.files():
            self.lazy.append(self.__las_to_parquet(las))


    def read_las(self, las):
        '''Reads las returns points as df.'''
        # make pipe, execute, extract array
        pipeline = pdal.Pipeline()
        pipeline |= pdal.Reader.las(filename=las)
        n = pipeline.execute()
        arr = pipeline.arrays[0]

        # make into df
        df = pd.DataFrame(arr)
        
        # sort values, make tile name, multi-index 
        df = df.sort_values(['X','Y', 'Z'])
        tile = f'{df.X.min()}_{df.Y.max()}_'
        df = df.set_index(['X','Y', 'Z'])

        return df, tile

    __read_las = read_las


    @delayed
    def las_to_parquet(self, las):
        '''Appends future parquet writes to self.lazy'''
        
        df, tile = self.__read_las(las)

        # path to tile
        tile_path = os.path.join(self.parquet_dir, f'{tile}.parquet')

        # append the to parquet
        return df.to_parquet(tile_path)

    __las_to_parquet = las_to_parquet



    def make_parquets(self):
        '''Computes the parquet futures'''

        if os.path.isdir(self.parquet_dir):
            shutil.rmtree(self.parquet_dir)

        os.makedirs(self.parquet_dir)

        with ProgressBar():
            _ = compute(*self.lazy)



    def see_what_files(self, data_dir, dimensions='all'):
        '''Finds supported files in data_dir, returns list.'''

        # read the directory
        data = os.listdir(data_dir)

        # look for copcs, laz and las
        copc = [os.path.join(data_dir, d) for 
                d in data if 
                d.endswith('.copc.laz')]

        las  = [os.path.join(data_dir, d) for 
                d in data
                if (not d.endswith('.copc.laz') and
                    d.endswith('.laz')) or
                d.endswith('las')]

        # until a copc reader is implemented just make excuse
        if len(copc) > 0:
            print('COPC reader not yet implemented. Reading COPC files as las.')
            las = las + copc

        self.files = las.copy
    
    __see_what_files = see_what_files


# %%
crs = 'EPSG:6339'
test_data = '/home/michael/work/pyDr/test_data'
OUT_PATH = Path('/home/michael/work/pyDr/test_output')

#%%
#%%


    # make each las into a parquet seperately, may have to rechunk each las if too big
#    ddf = dask_geopandas.from_geopandas(df, npartitions=4)