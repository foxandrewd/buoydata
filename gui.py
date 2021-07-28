#  --- imports ---

import os, sys, yaml, io
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
        self.master=tk.Tk
        self.app = self.master()

        #self.v=5       --  
    def __call__(self):
        print("Instance Window Called")
        #stub to wrap up user and config defined information and post it towards 
        self.parent()

    def parent(self):
        self.app.mainloop()
        return self.app
    
    def child(self,number):
        print("testing child")
        for i in range(number):
            self.instance[i]=tk.Toplevel(self.app)
            print(i)
        self.instance.mainloop()
        print("this should only run when tk is closed!")


#  ---
if __name__== "__main__":
    #           __init__
    rt=app_GUI()
    #           __call__
    #rt()
    rt.child(;;;;,,,,````````,,sfvbrtbrtbt,,,;)
    #parent()
    #rt.parent()
