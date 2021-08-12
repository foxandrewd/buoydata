#  --- imports ---

import os, sys, yaml, io
import tkinter as tk
import tkinter.ttk as ttk
from tkinter import filedialog

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
        self.mainloop()
        print("this should only run when tk is closed!")

def chooseCSVFile(event=None):
    global fname, fnameVar
    filename = filedialog.askopenfilename(initialdir=os.getcwd(), \
                            title = "Select CSV file",filetypes = (("CSV files","*.csv"),("All files","*.*")))
    print('Selected:', filename)
    
    fname = filename
    
    fnameVar.set(fname)
    return filename

#  ---
if __name__== "__main__":
    root = tk.Tk()
    
    root.title("Data Generator")
    
    mainFrame = tk.Frame(root)
    
    global fname; fname = ""
    global fnameVar; fnameVar = tk.StringVar()
    num_buoys = tk.StringVar()
    num_buoys.set("20")
    
    csv_folder = tk.StringVar()
    csv_folder.set("csv")
    
    sim_prefix = tk.StringVar()
    sim_prefix.set("simdata_")

    data_freq = tk.StringVar()
    data_freq.set("1")

    output_years = tk.StringVar()
    output_years.set("2022-2025")

    output_months = tk.StringVar()
    output_months.set("1-12")

    l = tk.Label(root, text="NDBC Buoy Data Generator", \
                       font = "sans 16 bold")
    
    l.pack()
    mainFrame.pack()
    
    L0 = tk.Label(mainFrame, text="Basis/Example CSV File:", font = "sans 10 bold")
    E0 = tk.Entry(mainFrame, width=60, textvariable=fnameVar)
    B0 = tk.Button(mainFrame, text="Select Basis CSV File", command = chooseCSVFile, \
                               font = "sans 10 bold")
    
    
    L1 = tk.Label(mainFrame, text="Num. of Buoys to Run:", font = "sans 10 bold")
    E1 = tk.Spinbox(mainFrame, justify=tk.CENTER, width=14, textvariable = num_buoys, from_=1, to=999999)

    L2 = tk.Label(mainFrame, text="CSV Folder Name:", font = "sans 10 bold")
    E2 = tk.Entry(mainFrame, width=40, textvariable = csv_folder)
    
    
    L3 = tk.Label(mainFrame, text="Simdata Filename Prefix:", font = "sans 10 bold")
    E3 = tk.Entry(mainFrame, width=40, textvariable = sim_prefix)
    
    
    L4 = tk.Label(mainFrame, text="Data Freq in Hours:", font = "sans 10 bold")
    E4 = tk.Spinbox(mainFrame, justify=tk.CENTER, width=14, textvariable = data_freq, from_=1, to=24)

    L5 = tk.Label(mainFrame, text="Output Years (start-end):", font = "sans 10 bold")
    E5 = tk.Entry(mainFrame, width=40, textvariable = output_years)

    L6 = tk.Label(mainFrame, text="Output Months (start-end):", font = "sans 10 bold")
    E6 = tk.Entry(mainFrame, width=40, textvariable = output_months)

    B7 = tk.Button(mainFrame, text="Generate Data", command = None, \
                               font = "sans 12 bold")
    
    L0.grid(row=0, column=0)
    E0.grid(row=0, column=1); B0.grid(row=0, column=2)
    
    L1.grid(row=1, column=0)
    E1.grid(row=1, column=1)    

    L2.grid(row=2, column=0)
    E2.grid(row=2, column=1)    

    L3.grid(row=3, column=0)
    E3.grid(row=3, column=1)    

    L4.grid(row=4, column=0)
    E4.grid(row=4, column=1)    
    
    L5.grid(row=5, column=0)
    E5.grid(row=5, column=1)    

    L6.grid(row=6, column=0)
    E6.grid(row=6, column=1)    
 
    B7.grid(row=7, column=1)

    root.mainloop()
