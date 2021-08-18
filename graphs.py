# -*- coding: utf-8 -*-
"""
Created on Wed Aug 18 13:07:59 2021

@author: Reuben
"""

import tkinter as tk
from PIL import Image

master = tk.Tk()

info_frame = tk.Frame(master).pack()
graph_frame = tk.Frame(master).pack()

def export():
    top = tk.Toplevel()
    
    var1 = tk.IntVar()
    var2 = tk.IntVar()
    var3 = tk.IntVar()
    
    graph1 = tk.Checkbutton(top, text="GRAPH TYPE 1", variable=var1).pack()
    graph2 = tk.Checkbutton(top, text="GRAPH TYPE 2", variable=var2).pack()
    graph3 = tk.Checkbutton(top, text="GRAPH TYPE 3", variable=var3).pack()
    
    def cancel():
        top.destroy()
        
    
    export_b = tk.Button(top, text="export").pack()
    cancel_b = tk.Button(top, text="Cancel",command=cancel).pack()
    
menubar = tk.Menu(master)
filemenu = tk.Menu(menubar, tearoff=0)
filemenu.add_command(label="Load as")
filemenu.add_command(label="Export", command=export)


filemenu.add_command(label="Exit", command=master.quit)
menubar.add_cascade(label="File", menu=filemenu)



master.config(menu=menubar)
master.mainloop()