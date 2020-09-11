import os
import win32com.client

srcDB = r'C:\Users\hswerdfe\Desktop\COVID-19_v2.accdb'
destDB = r'C:\Users\hswerdfe\Desktop\COVID-19_v2_backup.accdb'

oApp = win32com.client.Dispatch("Access.Application")
oApp.compactRepair(srcDB, destDB)
oApp.Application.Quit()
oApp = None

os.remove(srcDB)
os.rename(destDB, srcDB)
