import os
import paramiko
from datetime import datetime, timedelta

USER_FTP_HOST = "sftp.hydrognss.org"
USER_FTP_PORT=2222
USER_FTP_USERNAME = "username"
USER_FTP_PASSWORD = "password"

DOWNLOAD_ROOT_FOLDER = r"C:\SSTL\DownloadTest"

def getFormattedElaspsedTime(elapsed_time:timedelta):
    timeStr = str(elapsed_time)
    timeStr = timeStr[:-3]
    strArray = timeStr.split(':')
    if len(strArray) > 1 and strArray[0] == "0":
        timeStr = timeStr[2:]

    return timeStr

def getDateTimeFolder(startTime:datetime, endTime:datetime)->str:
    midPointTime = startTime + (endTime - startTime) / 2

    return midPointTime.strftime("%Y-%m/%d/H%H")
def getDownloadFolderForTimeAndModule(productFolder:str, satellite:str, startTime:datetime, endTime:datetime)->str:

    # First get the date/time folder structure

    dateTimeFolder = getDateTimeFolder(startTime, endTime) # applies to all L1 and L2 products

    return satellite + '/' + productFolder + '/' + dateTimeFolder

def getSftpDirListing(sftp: paramiko.SFTPClient)->list:
    dirListing = []
    try:
        attrsList = sftp.listdir_attr()

        for attrs in attrsList:
            dirListing.append(attrs.filename)
    except:
        dirListing = []

    return dirListing

def downloadProductsFromFolder(productFolder:str, satellite:str, startTime:datetime, endTime:datetime):

    downloadFolder = getDownloadFolderForTimeAndModule(productFolder, satellite, startTime, endTime)

    try:
        # create ssh connection:
        transport = paramiko.Transport((USER_FTP_HOST, int(USER_FTP_PORT)))
        transport.connect(username=USER_FTP_USERNAME, password=USER_FTP_PASSWORD)
        transport.packet_size = 1048576
        transport.window_size = 1073741824 
        transport.set_keepalive(30) # Prevents timeout drops during long downloads

        # create an SFTP client object
        sftp = paramiko.SFTPClient.from_transport(transport)

        folderOnSftpSite = '/' + downloadFolder

        # cd to the directory on the FTP site, creating sub-folders if they don't exist
        sftp.chdir(folderOnSftpSite)

        # Now download all the files from this folder on the SFTP site:

        # List of filenames in the folder on SFTP site

        filenames = getSftpDirListing(sftp)

        # Set up local folder to download the files to:
        downloadToFolder = os.path.join(DOWNLOAD_ROOT_FOLDER, downloadFolder)

        if not os.path.exists(downloadToFolder):
            os.makedirs(downloadToFolder)

        print(f"Downloading {filenames} from {folderOnSftpSite} on HydroGNSS SFTP site")
 
        for filename in filenames:

            localFilePath = os.path.join(downloadToFolder, filename)

            #if os.path.isfile(filePath) and filename.endswith('.nc'):
            filePathOnSftp = folderOnSftpSite + '/' + filename

            # Delete any previous version of the file, as we need to make sure the download was successful.
            # This way if the file is on the local disk afterwards, it must have just downloaded,
            # rather than the upload having failed, and the previous file being still on there.

            if os.path.exists(localFilePath):
                os.remove(localFilePath)

            # sftp.get(filePathOnSftp, localFilePath)
            
            # Open remote file in read-binary, local file in write-binary
            with sftp.open(filePathOnSftp, 'rb') as remote_file:
                # Disable default prefetch if it causes memory bloating or freezes
                remote_file.set_pipelined(True) 
                
                with open(localFilePath, 'wb') as local_file:
                    # Stream in 1MB chunks
                    chunk_size = 1024 * 1024 
                    while True:
                        chunk = remote_file.read(chunk_size)
                        if not chunk:
                            break
                        local_file.write(chunk)

        # Check all the files downloaded successfully:
        localDirListing = os.listdir(downloadToFolder)

        failedDownloadFiles = []
        for filename in filenames:
            if not filename in localDirListing:
                failedDownloadFiles.append(filename)

        if len(failedDownloadFiles) > 0:
            print(f"Failed to download files {failedDownloadFiles} from {folderOnSftpSite} on HydroGNSS SFTP site")
        else:
            print(f"All files downloaded from {folderOnSftpSite} on HydroGNSS SFTP site")

    except Exception as e:
        print(f"Download from HydroGNSS SFTP failed for {downloadFolder}: {e}")
        
    transport.close()

def downloadAll6HourProducts(startTime:datetime, endTime:datetime):

    downloadStartTime = datetime.now()
    print(f"Product download starting at " + downloadStartTime.isoformat(sep=' ', timespec='milliseconds'))

    pdgsProductFolders = ["L1A_L1B", "L2OP-FB", "L2OP-FT", "L2OP-SI", "L2OP-SSM", "L2OP-SWS"]
    
    satellites = ["HydroGNSS-1", "HydroGNSS-2"]

    for satellite in satellites:
        for productFolder in pdgsProductFolders:
            downloadProductsFromFolder(productFolder, satellite, startTime, endTime)

    downloadEndTime = datetime.now()

    # Calculate the elapsed time
    elapsed_time = downloadEndTime - downloadStartTime

    print(f"Product download completed successfully at " + downloadEndTime.isoformat(sep=' ', timespec='milliseconds'))
    timeStr = getFormattedElaspsedTime(elapsed_time)

    print(f"Product download time: " + timeStr + " seconds")

def main():

    # Download all 6-hour files between these dates
    startTime = datetime(2026, 8, 7, 21, 0, 0)
    endTime = datetime(2026, 8, 8, 3, 0, 0)

    downloadAll6HourProducts(startTime, endTime)

if __name__ == '__main__':

    main()
