#  --- imports ---

import os, sys, yaml, io
import tkinter as tk
import tkinter.ttk as ttk
from tkinter import filedialog
import buoy_generator as BG


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
    BASE_DIR_CHOOSE = "C:/Users/Barbf/Downloads/cmanwx"
    filename = filedialog.askopenfilename(initialdir = os.getcwd(),  ### initialdir=BASE_DIR_CHOOSE,
                 title = "Select CSV file",
                 filetypes = (("CSV files","*.csv"),("All files","*.*")))
    print('Selected:', filename)
    fname = filename
    fnameVar.set(fname)
    return filename

def runDataGenerator():
    params = {}
    yrstxt = output_years.get()
    if '-' in yrstxt:
        yrsarr = yrstxt.split('-')
        outyears_fixed = list(range( int(yrsarr[0]) , int(yrsarr[1])+1   ))
    else:
        outyears_fixed = [int(yrstxt)]
    params['outyears'] = outyears_fixed

    monstxt = output_months.get()
    monsarr = monstxt.split('-')
    outmonths_fixed = list(range( int(monsarr[0]) , int(monsarr[1])+1   ))
    params['outmonths'] = outmonths_fixed

    basis_fname = fnameVar.get()
    basis_fname = basis_fname[:basis_fname.rindex('/')]   # Chop off last bit #1
    
    basis_fname = basis_fname[:basis_fname.rindex('/')]     # Chop off last bit #2
    basis_month = basis_fname[(basis_fname.rindex('/')+1):]   # Last bit is now 'month'
    print('month: ', basis_month)
    params['basismonth'] = basis_month

    basis_fname = basis_fname[:basis_fname.rindex('/')]     # Chop off last bit #3
    basis_year = basis_fname[(basis_fname.rindex('/')+1):]    # Last bit is now 'month'
    print('year: ', basis_year)
    params['basisyear'] = basis_year
    basis_fname = basis_fname[:basis_fname.rindex('/')]     # Chop off last bit #4
    params['basisfolder'] = basis_fname                     # We're now down to the folder before year and month
    params['numbuoys'] = int(num_buoys.get())
    params['simprefix'] = sim_prefix.get().rstrip('_')
    params['datafreqinhours'] = int(data_freq.get().split(" ")[0]) # Integer value before the space char

    # Finally, run the data generation    
    BG.main(params)
    
    # Show finished
    tk.messagebox.showinfo(message="Data Generation is Complete.")


#  ---
if __name__== "__main__":
    root = tk.Tk()
    root.title("Buoy Data Generator")
    
    global fname
    fname = ""
    global fnameVar
    fnameVar = tk.StringVar()
    
    num_buoys = tk.StringVar()
    num_buoys.set("2")
    
    csv_folder = tk.StringVar()
    csv_folder.set("csv")
    
    sim_prefix = tk.StringVar()
    sim_prefix.set("simdata_")

    data_freq = tk.StringVar()
    data_freq.set("1 Hour")

    output_years = tk.StringVar()
    output_years.set("2022-2025")

    output_months = tk.StringVar()
    output_months.set("1-12")

    l = tk.Label(root, text="NDBC Buoy Data Generator", \
                       font = "sans 15 bold")
    mainFrame = tk.Frame(root)
    
    freqs_allowed = ("1 Hour", "2 Hours", "4 Hours",
                     "6 Hours", "8 Hours", "12 Hours", 
                     "24 Hours")
    
    l.pack()
    mainFrame.pack()
    
    s = ttk.Style()
    s.configure('my.TButton', font=('Sans', 10, 'bold'))
    
    L0 = tk.Label(mainFrame, text="Basis/Example CSV File:", font = "sans 10 bold")
    E0 = tk.Entry(mainFrame, width=54, textvariable=fnameVar)
    B0 = ttk.Button(mainFrame, text="Select Basis CSV File", command = chooseCSVFile,
                    style='my.TButton') #, font = "sans 10 bold")
    
    
    L1 = tk.Label(mainFrame, text="# Buoys to Generate:", font = "sans 10 bold")
    E1 = ttk.Spinbox(mainFrame, justify=tk.CENTER, width=14, textvariable = num_buoys, from_=1, to=1e7)

    L2 = tk.Label(mainFrame, text="CSV Folder Name:", font = "sans 10 bold")
    E2 = tk.Entry(mainFrame, width=40, textvariable = csv_folder)
    
    L3 = tk.Label(mainFrame, text="Simdata Filename Prefix:", font = "sans 10 bold")
    E3 = tk.Entry(mainFrame, width=40, textvariable = sim_prefix)
    
    L4 = tk.Label(mainFrame, text="Data Frequency (Hrs):", font = "sans 10 bold")
    E4 = ttk.Spinbox(mainFrame, justify=tk.CENTER, width=14, textvariable = data_freq,
                        values = freqs_allowed )

    L5 = tk.Label(mainFrame, text="Output Years: start-end", font = "sans 10 bold")
    E5 = tk.Entry(mainFrame, width=40, textvariable = output_years)

    L6 = tk.Label(mainFrame, text="Output Months: start-end", font = "sans 10 bold")
    E6 = tk.Entry(mainFrame, width=40, textvariable = output_months)

    L7 = tk.Label(mainFrame, text="    ", font = "sans 2")

    s2 = ttk.Style()
    s2.configure('my2.TButton', font=('Sans', 11, 'bold'))

    B8 = ttk.Button(mainFrame, text="Generate Buoy Data", command = runDataGenerator,
                              style = 'my2.TButton') ###font = "sans 11 bold")

    L9 = tk.Label(mainFrame, text="    ", font = "sans 2")
        
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

    L7.grid(row=7, column=0) 
    B8.grid(row=8, column=1)
    L9.grid(row=9, column=0)

    root.mainloop()
