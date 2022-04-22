import schedule
import time
import threading
import warnings

from alphadefi_fund import apiUpdate
from data_mirrorPlus import job as data_mirrorPlus
from data_liqProfile import job as data_liqProfile
from data_liqProfileHist import job as data_liqProfileHist
from data_aprMaster import job as data_aprMaster
from data_liqTxs import job as data_liqTxs
from data_liqStaking import job as data_liqStaking

warnings.filterwarnings("ignore")


def run_threaded(job_func):
    job_thread = threading.Thread(target=job_func)
    job_thread.start()


if __name__ == "__main__":

    # every minute, replace live liquidation data
    schedule.every().minute.at(":00").do(run_threaded, data_liqProfile)

    # every three minutes check for new liquidation txs
    schedule.every(3).minutes.at(":00").do(run_threaded, data_liqTxs)

    # every 1 hours save historical liquidation data 
    schedule.every(1).hours.at(":00").do(run_threaded, data_liqProfileHist)
    schedule.every(1).hours.at(":00").do(run_threaded, data_liqStaking)

    # every 4 hours, save master apr data and mirror data 
    schedule.every(4).hours.at(":00").do(run_threaded, data_mirrorPlus)
    schedule.every(4).hours.at(":00").do(run_threaded, data_aprMaster)

    #every day update daily low frequency data 
    schedule.every().day.at("00:10").do(run_threaded, apiUpdate)
    
    while True:
        schedule.run_pending()
        time.sleep(30)
