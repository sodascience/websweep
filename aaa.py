import re
pattern = re.compile(r"""
                            (financieel|rapportage|financial|annual.?report|jaarrekening|jaar.?verslag|jaarrapport|jaarrekening|boekhouding.?rapportage|boekhouding.?rapport|performance|boekhouding|investor|investeerder|results|AR)
                            """, re.VERBOSE | re.IGNORECASE)
neg_pattern = re.compile(r"""
                            medewerker|studeren|slim|algemene.?voorwaarden|privacy
                            """, re.VERBOSE | re.IGNORECASE)

AList = ["Dit is een string met een aantal dingen, waaronder financieel","Dit is een string met algemene voorwaarden", 'https://www.novatrace.nl/wp-content/uploads/2019/11/Novatrace-brochure.pdf','https://www.speedcargo.nl/wp-content/uploads/2021/12/Brandstoftoeslag-Speedcargo.pdf'] 
for link in AList:
    print(re.search(pattern, link))
    if re.search(pattern, link) and not re.search(neg_pattern, link):
        print(link)
 