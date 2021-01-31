#!/usr/bin/python
# coding: utf-8

import sqlite3
import datetime
import os
import shutil
import sys
import logging
import time

import sys, getopt

from math import floor 
from contextlib import closing

# TODO: Revisar que estoy tomando correctamente las versiones y sus fechas.
#       Ademas hay que poner fileisreference a true en todas
#       Por ultimo hay que enocontrar una forma de seleccionar el volumeId
#       Cuando se trate de una unidad externa.

# Apple considera el 1 de Enero de 2001 como partida del tiempo. epoch 978307200
epochOffset = 978307200
createFoldersAndCopyFiles = False
alterTables = False
showProgress = False

timestr = time.strftime("%Y%m%d-%H%M%S")
fmtstr = "%(levelname)s: %(asctime)s - %(message)s"
datestr = "%d/%m/%Y %I:%M:%S %p"

basePath = os.getcwd() #Ruta desde la que se ejecuta la aplicacion

#Creo una subcarpeta llamada logs para guardar los registros de ejecución
logPath = os.path.join(basePath,"logs")
if not os.path.exists(logPath): os.makedirs(logPath)
logFileName = "pet %s.log" % timestr
logFile = os.path.join(logPath,logFileName)

logging.basicConfig(
    filename=logFile,
    level=logging.DEBUG,
    fileMode="w",
    format=fmtstr,
    datefmt=datestr,
)

def dropTriggerMaster():
    return "drop trigger IF EXISTS RKMaster_ridIndexUpdateTrigger;"
def dropTriggerVersion():
    return "drop trigger IF EXISTS RKVersion_ridIndexUpdateTrigger;"
def createTriggerMaster():
    return "CREATE TRIGGER RKMaster_ridIndexUpdateTrigger after update of fileIsReference,isMissing,importedBy,originatingAssetIdentifier,fingerprint,isInTrash,volumeId,hasBeenSynced,modelId,isCloudQuarantined,cloudLibraryState,importComplete,photoStreamTagId,isExternallyEditable,isTrulyRaw,hasAttachments,hasCheckedMediaGroupId on RKMaster begin select RKMaster_notifyRidIndexUpdate(new.modelId,old.fileIsReference,old.isMissing,old.importedBy,old.originatingAssetIdentifier,old.fingerprint,old.isInTrash,old.volumeId,old.hasBeenSynced,old.modelId,old.isCloudQuarantined,old.cloudLibraryState,old.importComplete,old.photoStreamTagId,old.isExternallyEditable,old.isTrulyRaw,old.hasAttachments,old.hasCheckedMediaGroupId,new.fileIsReference,new.isMissing,new.importedBy,new.originatingAssetIdentifier,new.fingerprint,new.isInTrash,new.volumeId,new.hasBeenSynced,new.modelId,new.isCloudQuarantined,new.cloudLibraryState,new.importComplete,new.photoStreamTagId,new.isExternallyEditable,new.isTrulyRaw,new.hasAttachments,new.hasCheckedMediaGroupId) from RKMaster where rowid=old.rowid; end;"
def createTriggerVersion():
    return "CREATE TRIGGER RKVersion_ridIndexUpdateTrigger after update of isCloudQuarantined,videoCpVisibilityState,supportedStatus,colorSpaceValidationState,showInLibrary,fileIsReference,isFavorite,isInTrash,isHidden,faceDetectionIsFromPreview,hasKeywords,subType,specialType,momentUuid,burstPickType,graphProcessingState,type,mediaAnalysisProcessingState,playbackStyle,playbackVariation,renderEffect,groupingState,selfPortrait,outputUpToDate,syncFailureHidden,modelId,searchIndexInvalid,cloudLibraryState,hasBeenSynced on RKVersion begin select RKVersion_notifyRidIndexUpdate(new.modelId,old.isCloudQuarantined,old.videoCpVisibilityState,old.supportedStatus,old.colorSpaceValidationState,old.showInLibrary,old.fileIsReference,old.isFavorite,old.isInTrash,old.isHidden,old.faceDetectionIsFromPreview,old.hasKeywords,old.subType,old.specialType,old.momentUuid,old.burstPickType,old.graphProcessingState,old.type,old.mediaAnalysisProcessingState,old.playbackStyle,old.playbackVariation,old.renderEffect,old.groupingState,old.selfPortrait,old.outputUpToDate,old.syncFailureHidden,old.modelId,old.searchIndexInvalid,old.cloudLibraryState,old.hasBeenSynced,new.isCloudQuarantined,new.videoCpVisibilityState,new.supportedStatus,new.colorSpaceValidationState,new.showInLibrary,new.fileIsReference,new.isFavorite,new.isInTrash,new.isHidden,new.faceDetectionIsFromPreview,new.hasKeywords,new.subType,new.specialType,new.momentUuid,new.burstPickType,new.graphProcessingState,new.type,new.mediaAnalysisProcessingState,new.playbackStyle,new.playbackVariation,new.renderEffect,new.groupingState,new.selfPortrait,new.outputUpToDate,new.syncFailureHidden,new.modelId,new.searchIndexInvalid,new.cloudLibraryState,new.hasBeenSynced) from RKVersion where rowid=old.rowid; end;"
def masterSQLUpdateCommand(masterId,newImagePath,newFileIsReference): 
    return "UPDATE RKMaster SET imagePath='{imagePath}', fileIsReference={fileIsReference}, volumeId=3 WHERE modelId={modelId};".format(imagePath=newImagePath, fileIsReference=newFileIsReference, modelId=masterId)
def versionSQLUpdateCommand(masterId,newFileIsReference): 
    return "UPDATE RKVersion SET fileIsReference={fileIsReference} WHERE modelId={modelId};".format(fileIsReference=newFileIsReference, modelId=masterId)

def getDatabaseRows(databaseFile):
    sqlCommand = """
        select
            RKMaster.modelId,
            RKMaster.imagePath,
            mRKVersion.imageDate,
             datetime(mRKVersion.imageDate+978307200,'unixepoch') as VersionDate,
            RKMaster.imageDate,
            datetime(RKMaster.imageDate+978307200,'unixepoch') as masterDate
        FROM 
            RKMaster LEFT JOIN (select RKVersion.masterId as masterId, RKVersion.imageDate as imageDate FROM RKVersion group by RKVersion.masterId having max(modelId) order by modelId desc) as mRKVersion
        ON RKMaster.modelid=mRKVersion.masterid
        WHERE RKMaster.fileIsReference=0;
        """
    with closing(sqlite3.connect(databaseFile)) as connection:
        with closing(connection.cursor()) as cursor:
            rows = cursor.execute(sqlCommand).fetchall()
    return rows 
    
def datetime_to_float(d):
    epoch = datetime.datetime.utcfromtimestamp(0)
    total_seconds =  (d + epochOffset).total_seconds()
    return total_seconds
def float_to_datetime(fl):
    return datetime.datetime.fromtimestamp(fl + epochOffset)

def showHelp():
    ### Metodo para imprimir la ayuda del programa ###
    print('\nPhotos Extract Tool copy the files inside your library to a new location specified by you.\n')
    print('Some comsiderations:\n')
    print('   - Make a backup or work on a copy of your library')
    print('   - It works on Apple Photos app version 3.0 (3291.13.230)')
    print('   - Python is 2.7 included in High Sierra and has been tested under macOS 10.13.6')
    print('   - Only imported files to library will be copied')
    print('   - output structure will geberated based on date and time from library, not files')
    print('   - you cant test the process quickly not including -e and -t tables')
    print('   - you should use -t -e both together, anyway both are avaliable for other purposes\n')
    print('Usage:\n')
    print('   pet.py -i <inputlibrary> -o <outputpath>')
    print('   pet.py --library <inputlibrary> --outputpath <outputpath>\n')
    print('Commands:')
    print('   -i, --library       Should be a photos library file folder path')
    print('   -o, --outputpath    Absolute path where the photos will be exported as year/month folder\'s hierarchy')
    print('   -e, --exportfiles   Copy files to outputPath if ommited files will not be copied')
    print('   -t, --alterTables   Modify the library to link to exported files. Use with caution. Make a backup before use')
    print('   -v                  Show progress\n')
    print('   -h                  help\n')
    print('examples:\n')
    print('   pet.py -i /Users/user1/Pictures/myPhotos.photoslibrary/ -o /Volumes/myUSBLabel/')
    print('   pet.py --library /Users/user1/Pictures/myPhotos.photoslibrary/ --outputPath /Volumes/myUSBLabel/\n')
    sys.exit(2)
def extractArguments(argv):
    ### Extrae los argumentos de la linea de comandos y los devuelve en forma de lista (Libreria, carpeta) ###
    # TODO: Refactorizar, se puede dejar en el 50% de líneas seguro
    libraryFolder = ''
    outputPath = ''
    try:
        opts, args = getopt.getopt(argv,"vheti:o:",["library=","outputpath=","--exportfiles","--alterTables"])
        if len(opts)==0:
            showHelp()
    except getopt.GetoptError:
        showHelp
    parametersOk = 0
    for opt, arg in opts:
        if opt == '-h':
            showHelp()
        elif opt in ("-i", "--library"):
            if os.path.exists(arg):
                libraryFolder = os.path.join(arg, "Masters")
                databaseFile = os.path.join(arg,"database",'photos.db')
                parametersOk += 1
            else:
                print('\nError - Library Folder is not a valid path!!!\n')
                sys.exit(2)
        elif opt in ("-o", "--outputpath"):
            if os.path.exists(arg):
                outputPath = arg
                parametersOk += 1
            else:
                print('\nError - outPath Folder is not a valid path!!!\n')
                sys.exit(2)
        elif opt in ("-e", "--exportfiles"):
            global createFoldersAndCopyFiles
            createFoldersAndCopyFiles = True
        elif opt in ("-t", "--alterTables"):
            global alterTables
            alterTables = True
        elif opt in ("-v"):
            global showProgress
            showProgress = True
    if parametersOk == 2:
        return (databaseFile, libraryFolder,outputPath)
    else:
        showHelp()

def main(argv):

    fileErrors = 0
    tableErrors = 0

    databaseFile, mastersPath, exportPath = extractArguments(argv)

    inicio = datetime.datetime.now()
    logging.info("Process started at %s" % (inicio))
    logging.info("Execution path is %s" % basePath)
    logging.info("Masters path: %s" % mastersPath)
    if exportPath: logging.info("and will be exported to path: %s" % exportPath)

    connection = sqlite3.connect(databaseFile)
    cursor = connection.cursor()

    #Borrar los Triggers
    cursor.execute(dropTriggerMaster())
    cursor.execute(dropTriggerVersion())

    rows = getDatabaseRows(databaseFile)
    for idx,row in enumerate(rows):
        modelId = row[0]
        logging.debug("processing %i" % modelId)
        try:
            index = (2, 4)[row[2] is None] #Select RKVersion.imageDate if is None RKMaster.imageDate
            fecha = float_to_datetime(row[index])
        except:
            errorMsg = ("La conversion de la fecha no es posible. %s - %s para el elemento %d" % (row[2], row[3], row[0]))
            print(errorMsg)
            logging.error(errorMsg)
            fileErrors += 1
            continue
        try:
            imagePath = row[1].encode('UTF-8')
        except:
            errorMsg = ("La ruta contiene caracteres no convertibles. %s - para el elemento %d" % (row[1], row[3]))
            print(errorMsg)
            logging.error(errorMsg)
            fileErrors += 1
            continue

        imageName = os.path.basename(imagePath)
        folderYear = "%04d" % fecha.year
        folderMonth = "%02d" % fecha.month
        destinationFolder = "%s/%04d/%02d" % (exportPath, fecha.year, fecha.month) 
        if not os.path.exists(destinationFolder):
            if createFoldersAndCopyFiles:
                infoMsg = "Creating folder {destinationFolder}".format(destinationFolder=destinationFolder)
                logging.info(infoMsg)
                os.makedirs(destinationFolder)

        source = os.path.join(mastersPath, imagePath)
        target = os.path.join(exportPath, folderYear, folderMonth, imageName)
        try:
            if ((idx % 100) == 0) and showProgress:
                print("%s - %d/%d registers processed " % (datetime.datetime.now(),idx, len(rows))) 
            if createFoldersAndCopyFiles:
                logging.info("copying {source} to {target}".format(source=source,target=target))
                shutil.copy(source, target)
        except IOError as e:
            msgError = "Copia no realizada: %s" % source 
            logging.error(source)
            fileErrors += 1
        except:
            msgError = "*** Error inesperado: %s" % sys.exc_info()
            print(msgError)
            logging.error(msgError)
            quit()
        try:
            if alterTables:
                cursor.execute(masterSQLUpdateCommand(modelId,target,1))
                cursor.execute(versionSQLUpdateCommand(modelId,1))
        except:
            msgError = "Tabla no actualizada: %s para el elemento %i" % (source, modelId) 
            logging.error(source)
            tableErrors += 1

    #recrear los Triggers
    cursor.execute(createTriggerMaster())
    cursor.execute(createTriggerVersion())

    connection.commit() #Guardamos los cambios en la libreria (BBDD)
    connection.close() #Cerramos la conexion a la base de datos

    fin = datetime.datetime.now()
    tiempoEmpleado = fin-inicio
    segundos = tiempoEmpleado.total_seconds()
    horas = floor((tiempoEmpleado.total_seconds() / 3600))
    print("%s registers processed" % len(rows))
    print("%d errors copyng files" % fileErrors)
    print("%d errors modifyng library info" % tableErrors)
    print("Finished in %d seconds" %(segundos))
    print("(Arround) %d hours" % horas)
    logging.info("Process finished at %s with %d errors in %d seconds" % (fin, fileErrors, segundos))

if __name__ == "__main__":
    main(sys.argv[1:])

# TODO: Es necesario crear un registro en RKVolume con los siguientes datos
#        modelId = 2
#          uuid = nAXmZh%wQpennq+jbp1pIg
#       modDate = 633549371.504304
#          name = WD1TB
#    createDate = 633549371.504304
#    bookmarkId = 3
#      diskUuid = 8145CF84-DEF9-3B1A-81A8-EF66D66EDAE1
#         label = 
#     isOffline = 0
# bookmarkSSBId = 
# Tal vez lo mas sencillo sería listar los existentes en la base de datos y dar la opción de escoger uno.
# En mi caso voy a exportar el registro y a importarlo en la base de datos original
# Posteriormente voy a asignar este identificador a las fotos. (Estudiarlo o analizarlo mas en profundidad)
