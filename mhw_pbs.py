# JR 2023-01-12
# compute Tasman Sea temp index from NOAA OItemp v2.1

# load python modules
print("loading modules")
import xarray as xr
import numpy as np
import os
# from glob import glob
from timeit import default_timer as timer
import sys

from dask.distributed import Client
# load required modules
from datetime import timedelta
import glob
from xmhw.xmhw import threshold, detect

def print_run_time(time):
    print(f"Elapsed (wall) time: {str(timedelta(seconds=time))}")

# set up a Dask cluster
if __name__ == '__main__':
    
    start = timer()
    
    worker_dir=os.getenv('PBS_JOBFS')
    if not worker_dir:
        worker_dir=os.getenv('TMPDIR')
    if not worker_dir:
        worker_dir="/tmp"
    client = Client(local_directory=worker_dir)
    print(client.ncores)

    # load data
    print('Loading data ...')
    print_run_time(timer() - start)
    # open only the files you need and use parallel
    ################################################
    workdir = '/scratch/v45/jr5971/'
    data_name = 'eac_surface.zarr'
    # specify native coordinate names
    x = 'xh'
    y = 'yh'
    var_dict = {x:'lon', y:'lat'}
    ################################################
    print('Opening temp data')
    temp = xr.open_zarr(workdir + data_name)
    temp = temp.rename(var_dict)

    # Convert time to datetime from cftime
    temp['time'] = temp['time'].astype('datetime64[ns]')

    # print some data details;
    print(f'Dataset size is {temp.nbytes/1e6} MB')
    print(f'Dataset dims are: lon:{len(temp.lon)}, lat:{len(temp.lat)}')
    print(f'Dataset lat min/max are {np.round(temp.lat.min().values,2)}, {np.round(temp.lat.max().values,2)}')

    print('Chunking data')
    #### Chunk and Split-By_Chunk ####
    # This is the bit that'd require the most changing - better off working this out in a notebook
    ny_blocks = 308//2    # Note 308 is no. lat cells
    y_chunks = len(temp.lat) // ny_blocks
    nx_blocks = 1 # Chunks span all x
    x_chunks = len(temp.lon) // nx_blocks
    ##Chunk
    temp = temp.chunk({'time':-1, 'lon':x_chunks, 'lat':y_chunks})
    # now you can subtract
    #temp = temp - 273.15

    '''
    So the above has broken up the regional domain into 16 latitudinal chunks. Next, in order
    to split by chunk, we figure out the general dimensions of a chunk e.g., how many cells
    in x/y. 
    '''

    _, yt, xt = temp.chunks
    xtstep = xt[0]
    ytstep = yt[0]
    xtblocks = len(xt)
    ytblocks = len(yt)
    print(f'block pieces are full-width in x ({xtstep} cells and have {ytstep} lat cells each | number of chunks in y is {ytblocks}')

    #### MHW threshold function ####
    print('Computing threshold function')
    start_full=timer()
    # do not load here the entire array in memory, when you are using ony part of it at one time
    #temp = temp2.load()
    results_th = []
    results_det = []
    results_int = []
    for j in range(ytblocks):
        start_loop = timer()
        yt_from = j*ytstep
        yt_to = (j+1)*ytstep
        ts = temp.isel(lat=slice(yt_from, yt_to)).load()
        ds_th = threshold(ts)
        results_th.append(ds_th)
        print(f'threshold {j+1} of size {ts.nbytes/1e6}MB is finished! Time:{str(timedelta(seconds=timer()-start_loop))}')
        results_th[j].to_netcdf(f'{workdir}temp_res/th_res_{j}.nc')
        print('and saved!')

        ds_th = xr.open_dataset(f'{workdir}temp_res/th_res_{j}.nc')
        th = ds_th['thresh']
        se = ds_th['seas']
        print('running detect function')
        mhw, intermediate = detect(ts, th=th, se=se, intermediate=True)
        results_det.append(mhw)
        results_int.append(intermediate)
        results_det[j].to_netcdf(f'{workdir}temp_res/det_res_{j}.nc')
        print(f'saving intermediate results number {j+1}')
        results_int[j].to_netcdf(f'{workdir}temp_res/int_res_{j}.nc')
        print(f'Chunk {j+1} is finished! Time:{str(timedelta(seconds=timer()-start_loop))}')
        del(ts,ds_th, th, se)
        j+=1





    # print('Writing results to file')
    
    print_run_time(timer() - start_full)
    
    # clim = xr.combine_by_coords(results)
    # clim.to_netcdf('/g/data/v45/jr5971/mhw-analysis/mhws_noaa-copy/clim_oitemp.nc')


