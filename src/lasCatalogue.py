#%%
import pdal
import dask.dataframe as dd
import dask.array as da
import dask_geopandas
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point

from dask import delayed, compute
import os


#%%

@delayed(pure=True)
def read_las(las):
    '''Reads las returns points as geodf.
    TODO: verbose mode, options?'''
    # make pipe, execute, extract array
    pipeline = pdal.Pipeline(pdal.Reader.las(filename=las))
    n = pipeline.execute()
    arr = pipeline.arrays[0]

    # make into df the geodf
    df = pd.DataFrame(arr)
    df = gpd.GeoDataFrame(df,
                          geometry=gpd.points_from_xy(df.X,
                                                      df.Y,
                                                      z=df.Z))

    return df


def multi_las_to_parquet(las_list):
    '''Reads list of las/z files. Creates future dask df'''
    
    # make list of delayed dfs
    lazy = [read_las(las) for las in las_list]

    #



def see_what_files(data_dir, dimensions='all'):
    '''Reads pointcloud using pdal'''

    # read the directory
    data = os.listdir(data_dir)

    # look for copcs, laz and las
    copc = [d for d in data if d.endswith('.copc.laz')]
    las  = [d for d in data
            if (not d.endswith('.copc.laz') and
                d.endswith('.laz')) or
            d.endswith('las')]

    # until a copc reader is implemented just make excuse
    if len(copc) > 0:
        print('COPC reader not yet implemented. Reading as las.')


    
# %%
if __name__ == '__main__':

    see_what_files()

    # make each las into a parquet seperately, may have to rechunk each las if too big
    ddf = dask_geopandas.from_geopandas(df, npartitions=4)