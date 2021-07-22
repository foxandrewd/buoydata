#  --- imports ---

import os, sys, yaml
import tkinter as tk
import tkinter.ttk as ttk
#  ---

#  --- global variable land ---
class varlib:
    def __init__(self):
        print("init start")
        #variables and declarations
        print("init end")

#  ---

#  --- window GUI ---
class app_GUI:
    def __init__(self):
        print("Parent Instance Created")
        
        self.v=5
    def __call__(self):
        print("Instance Window Called")
        self.parent()

    def parent(self):
        print(self.v)
    
#  ---

if __name__== "__main__":
    rt=app_GUI()          #should call __init__
    rt()                        #should call __call__
    rt.parent()               #should call parent()
