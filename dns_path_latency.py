import subprocess
import timeit
import numpy as np
import pandas as pd
import socket
from datetime import datetime

print(str(datetime.now()))


#df = pd.DataFrame({"domain":["espn.com"]})
'''
df = pd.DataFrame({"domain":["rfi.fr","cnn.com","cbs.com","abc.com",
                             "foxnews.com","cnbc.com", "www.youtube.com",
                             "www.live.com", "amazonaws.com","netflix.com",
                             "doubleclick.net", "www.facebook.com", "apple.com",
                             "skype.com", "live.com"]})
                             '''

#df = pd.DataFrame({"domain":["si.com","g.doubleclick.net"]})
df = pd.read_csv("C:\\3rd Semester\\RA\\script_for_Nikhil\\script_for_Nikhil\\top-200.csv", header=None, names = ["id","domain"])
#df = df.assign(trigger_measurement = 0)
#df = df.assign(local_Cache_Latency = 0)
#df = df.assign(ns = "")
#df = df.assign(ns_latency = 0)
#df = df.assign(ns_internet_hops = 0)
#df = df.assign(root_latency = 0)
#df = df.assign(recursive_resolver_Latency = 0)
df = df.assign(recursive_resolver = "WARP.uccs.edu")

cmd = 'ipconfig /flushdns'
subprocess.Popen(cmd,stdout=subprocess.PIPE)

def get_tld(domain):
    try:
        tld = domain.split(".").pop()
        return tld
    except:
        return -1
    
    
def query_rs_server(tld, rns):
    try:
        cmd = 'dig {} @{}' .format(tld, rns)
        proc=subprocess.Popen(cmd,stdout=subprocess.PIPE)
        out,err=proc.communicate()
        return int(out.decode().split("\r\n\r\n")[4].split(";;")[1].split(" ")[3])
    except:
        return -1
        
def query_tld_server(domain, tld_server):
    try:
        cmd = 'dig {} @{}' .format(domain, tld_server)
        proc=subprocess.Popen(cmd,stdout=subprocess.PIPE)
        out,err=proc.communicate()
        return int(out.decode().split("\r\n\r\n")[3].split(";;")[1].split(" ")[3])
    except:
        return -1
        
def query_ans_server(domain, ans_server):
    try:
        cmd = 'dig {} @{}' .format(domain, ans_server)
        proc=subprocess.Popen(cmd,stdout=subprocess.PIPE)
        out,err=proc.communicate()
        return int(out.decode().split("\r\n\r\n")[3].split(";;")[1].split(" ")[3])
    except:
        return -1

def query_record(domain):
    try:
        socket.gethostbyname(domain)
    except socket.gaierror:
        print(domain)
        pass
        
def return_rr(): 
    return "WARP.uccs.edu"
    
def query_ns_timeit(domain, ns):
    try:
        cmd = 'dig {} @{} A' .format(domain, ns)
        subprocess.Popen(cmd,stdout=subprocess.PIPE)
    except SyntaxError:
        print("syntax error", domain)

'''
def fastest_ns(domain):
    cmd = 'dig {} +short NS' .format(domain)
    proc=subprocess.Popen(cmd,stdout=subprocess.PIPE)
    out,err=proc.communicate()
    ns_arr = (out.decode().split("\r\n"))[:-1]
    ns_dict = dict()
    if (len(ns_arr) != 0):
        for i in ns_arr:
            latency = timeit.timeit('query_ns(\'{0}\', \'{1}\')' .format(domain, i), 'from __main__ import query_ns', number = 1)
            ns_dict.update({i:latency})
    else:
        ns_dict.update({"No Domain":-1})       
    k = min(ns_dict, key=lambda k: ns_dict[k])
    return k
 '''  
    
def count_hops(ns):
    cmd = 'tracert {}' .format(ns)
    proc=subprocess.Popen(cmd,stdout=subprocess.PIPE)
    out,err=proc.communicate()
    return len([x for x in out.decode().split("\n")]) - 7
        

def dns_root_latency(domain):
    cmd = 'dig {} +trace' .format(domain)
    proc=subprocess.Popen(cmd,stdout=subprocess.PIPE)
    out,err=proc.communicate()
    root_arr = []
    for i in range(2, len((out.decode().split("\r\n\r\n"))) - 1, 1):
        try:
            root_arr.insert(i-2, int(out.decode().split("\r\n\r\n")[i].split(";;")[1].split(" ")[7]))
        except ValueError:
            pass
        except IndexError:
            pass 
    
    if len(root_arr) == (len(out.decode().split("\r\n\r\n")) - 3):
        path_3_RR_ANS_RR = int(out.decode().split("\r\n\r\n")[3].split(";;")[1].split(" ")[7])
        path_4_RR_RS_RR_TLD_RR_ANS_RR = np.sum(root_arr) #+ int(out.decode().split("\r\n\r\n")[2].split(";;")[1].split(" ")[7])
        path_5_RR_RS_RR =  int(out.decode().split("\r\n\r\n")[1].split(";;")[1].split(" ")[7])
        path_6_RR_RS_RR_TLD_RR = int(out.decode().split("\r\n\r\n")[1].split(";;")[1].split(" ")[7]) + int(out.decode().split("\r\n\r\n")[3].split(";;")[1].split(" ")[7])
        rns = out.decode().split("\r\n\r\n")[1].split(";;")[1].split(" ")[5].split("(")[0].split("#")[0]
        RR_TLD_RR = int(out.decode().split("\r\n\r\n")[2].split(";;")[1].split(" ")[7])
        TLD_ns = out.decode().split("\r\n\r\n")[2].split(";;")[1].split(" ")[5].split("(")[0].split("#")[0]
        ANS_ns = out.decode().split("\r\n\r\n")[3].split(";;")[1].split(" ")[5].split("(")[0].split("#")[0]
        return path_3_RR_ANS_RR, path_4_RR_RS_RR_TLD_RR_ANS_RR, path_5_RR_RS_RR, path_6_RR_RS_RR_TLD_RR, rns, RR_TLD_RR, TLD_ns, ANS_ns
    else:
        return -1, -1, -1, -1, -1, -1, -1, -1
    
    
df["trigger_measurement"] = df.apply(lambda row: timeit.timeit('query_record("{}")' .format(row["domain"]), 'from __main__ import query_record', number = 1), 1)

df["local_Cache_Latency"] = df.apply(lambda row: timeit.timeit('query_record("{}")' .format(row["domain"]), 'from __main__ import query_record', number = 1), 1)

cmd = 'ipconfig /flushdns'
subprocess.Popen(cmd,stdout=subprocess.PIPE)
df.apply(lambda row: query_record(row["domain"]), 1)
cmd = 'ipconfig /flushdns'
subprocess.Popen(cmd,stdout=subprocess.PIPE)


df["recursive_resolver_Latency"] = df.apply(lambda row: timeit.timeit('query_record("{}")' .format(row["domain"]), 'from __main__ import query_record', number = 1), 1)
df["recursive_resolver_hops"] = df.apply(lambda row: count_hops(row["recursive_resolver"]), 1)

df["trace_attributes"] = df.apply(lambda row: dns_root_latency(row["domain"]), 1)

df["ans"] = df.apply(lambda row: (row["trace_attributes"])[7], 1)
#df["ans_latency_timeit"] = df.apply(lambda row: timeit.timeit('query_ns_timeit("{0}","{1}")' .format(row["domain"], row["ans"]), 'from __main__ import query_ns_timeit', number = 1), 1)    
df["ans_@_latency"] = df.apply(lambda row: query_ans_server(row["domain"], row["ans"]), 1)
df["ans_internet_hops"] = df.apply(lambda row: count_hops(row["ans"]), 1)

df["path_3_RR_ANS_RR"] = df.apply(lambda row: (row["trace_attributes"])[0], 1)

df["path_4_RR_RS_RR_TLD_RR_ANS_RR"] = df.apply(lambda row: (row["trace_attributes"])[1], 1)

df["path_5_RR_RS_RR"] = df.apply(lambda row: (row["trace_attributes"])[2], 1)
df["root_server"] =df.apply(lambda row: (row["trace_attributes"])[4], 1)
df["rns_hops"] = df.apply(lambda row: count_hops(row["root_server"]), 1)

df["path_6_RR_RS_RR_TLD_RR"] = df.apply(lambda row: (row["trace_attributes"])[3], 1)
df["tld_server"] =df.apply(lambda row: (row["trace_attributes"])[6], 1)
df["tld_hops"] = df.apply(lambda row: count_hops(row["tld_server"]), 1)

df["tld"] = df.apply(lambda row: get_tld(row["domain"]), 1)
df["rs_@_latency"] = df.apply(lambda row: query_rs_server(row["tld"], row["root_server"]), 1)
df["tld_@_latency"] = df.apply(lambda row: query_tld_server(row["domain"], row["tld_server"]), 1)

df["local_Cache_Latency"] = df["local_Cache_Latency"]*1000
df["recursive_resolver_Latency"] = df["recursive_resolver_Latency"]*1000
#df["ans_latency_timeit"] = df["ans_latency_timeit"]*1000

'''
df["ns"] = df.apply(lambda row: fastest_ns(row["domain"]), 1) 
df["ns_latency"] = df.apply(lambda row: timeit.timeit('query_ns("{0}","{1}")' .format(row["domain"], row["ns"]), 'from __main__ import query_ns', number = 1), 1)
df["ns_internet_hops"] = df.apply(lambda row: count_hops(row["ns"]), 1)

df["root_latency"] = df.apply(lambda row: (row["trace_attributes"])[0], 1)
df["root_server"] =df.apply(lambda row: (row["trace_attributes"])[1], 1)
df["rns_hops"] = df.apply(lambda row: count_hops(row["root_server"]), 1)

df["tld_latency"] = df.apply(lambda row: (row["trace_attributes"])[2], 1)
df["tld_server"] =df.apply(lambda row: (row["trace_attributes"])[3], 1)
df["tld_hops"] = df.apply(lambda row: count_hops(row["tld_server"]), 1)
'''
  
#df.to_csv("result.csv", columns = [df["local_Cache_Latency"],
#                                   df["recursive_resolver_Latency"], 
#                                   df["ns_latency"], df["ns_internet_hops"],
#                                   df["root_latency"]])
df.to_csv("C:\\3rd Semester\\RA\\script_for_Nikhil\\script_for_Nikhil\\result_6paths_100.csv")
                            
print(str(datetime.now())) 