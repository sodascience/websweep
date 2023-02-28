import requests
import socket
import json

key = open("ipinfokey.key").read()

def get_location(ip):
    # More detailed info on the host (50k queries/month)
    r = requests.get(f'http://ipinfo.io/{ip}?token={key}')
    others = json.loads(r.text)
    others = [others.get("hostname"), others.get("city"), others.get("region"), others.get("country"),
    others.get("loc"), others.get("org"), others.get("postal"), others.get("timezone")]
    return others

def get_host(domain):
    """
    Get information about the host
    """
    try:
        # Get IP of domain
        ip = socket.gethostbyname(domain)
        # Some info on the host
        host = socket.gethostbyaddr(ip)
    except:
        return [domain] + [""] * 11
    
    others = get_location(ip)
    others_add = get_location(host[1][0].replace(".in-addr.arpa",""))

    return [domain, ip, host[0], host[1][0]] + [str(_) for _ in others] + [str(_) for _ in others_add]


# with open("data/tmp_data/sidn_test.csv") as f, open("data/tmp_data/sidn_test_info.csv", "w+") as fout:
#     f.readline()
#     fout.write(f"Domain\tIP\tHost\tHost_addr\thostname\tcity\tregion\tcountry\tloc\torg\tpostal\ttimezone\thostname_in-addr\tcity_in-addr\tregion_in-addr\tcountry_in-addr\tloc_in-addr\torg_in-addr\tpostal_in-addr\ttimezone_in-addr\n")

#     for line in f:
#         domain = line.split(",")[0]
#         host = get_host(domain)
#         fout.write("\t".join(host)+"\n")

# TODO: change to config source file
import pandas as pd
df = pd.read_csv("data/tmp_data/sidn_test_info.csv", sep="\t")
for col in ["Host", "Host_addr", "hostname", "country", "org",  "hostname_in-addr", "country_in-addr",  "org_in-addr"]:
    if col == "Host":
        print(df[col].dropna().apply(lambda x: tuple(x.split(".")[-2:])).value_counts().head(10))
    else:
        print(df[col].dropna().value_counts().head(10))
    print("--"*40+"\n\n")
    