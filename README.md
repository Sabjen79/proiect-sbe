# Proiect Sbe

---

<div align="center">Florin Bălan / Ștefana Ciudin / Diana Jîtcă</div>

---

- [x] Generati un flux de publicatii care sa fie emis de un nod publisher. Publicatiile pot fi generate cu valori aleatoare pentru campuri folosind generatorul de date din tema practica. **<span style="color: blue;">(5 puncte)</span>**
- [x] Implementati o retea (overlay) de brokeri (2-3) care sa notifice clienti (subscriberi) in functie de o filtrare bazata pe continutul publicatiilor, cu posibilitatea de a procesa inclusiv ferestre (secvente) de publicatii (exemplu mai jos). **<span style="color: blue;">(10 puncte)</span>**
- [ ] Simulati 3 noduri subscriber care se conecteaza la reteaua de brokeri si pot inregistra atat susbcriptii simple cat si subscriptii complexe ce necesita o filtrare pe fereastra de publicatii. Subscriptiile pot fi generate cu valori aleatoare pentru campuri folosind generatorul de date din tema practica, modificat pentru a genera si subscriptii pentru ferestre de publicatii (exemplu mai jos). **<span style="color: blue;">(5 puncte)</span>**
- [ ] Folositi un mecanism de serializare binara (exemplu - Google Protocol Buffers sau Thrift) pentru transmiterea publicatiilor de la nodul publisher la brokers. **<span style="color: blue;">(5 puncte)</span>**
- [ ] Realizati o evaluare a sistemului, masurand pentru inregistrarea a 10000 de subscriptii simple, urmatoarele statistici: **<span style="color: blue;">(10 puncte)</span>**
    1. cate publicatii se livreaza cu succes prin reteaua de brokeri intr-un interval continuu de feed de 3 minute, 
    2. latenta medie de livrare a unei publicatii (timpul de la emitere pana la primire) pentru publicatiile trimise in acelasi interval, 
    3. rata de potrivire (matching) pentru cazul in care subscriptiile generate contin pe unul dintre campuri doar operator de egalitate (100%) comparata cu situatia in care frecventa operatorului de egalitate pe campul respectiv este aproximativ un sfert (25%). Redactati un scurt raport de evaluare a solutiei.
- [ ] Implementati un mecanism avansat de rutare la inregistrarea subscriptiilor simple ce ar trebui sa fie distribuite in reteaua de brokeri (publicatiile vor trece prin mai multi brokeri pana la destinatie, fiecare ocupandu-se partial de rutare, si nu doar unul care contine toate subscriptiile si face un simplu match). **<span style="color: green;">(5 puncte)</span>**
- [ ] Simulati si tratati (prin asigurare de suport in implementare) cazuri de caderi pe nodurile broker, care sa asigure ca nu se pierd notificari, inclusiv pentru cazul subscriptiilor complexe. **<span style="color: green;">(5 puncte)</span>**
- [ ] Implementati o modalitate de filtrare a mesajelor care sa nu permita brokerilor accesul la continutul mesajelor (match pe subscriptii/publicatii criptate). **<span style="color: green;">(5-10 puncte)</span>**

# Exemplu

Exemplu filtrare subscriptii simple si subscriptii complexe (cu filtrare pe fereastra de publicatii):

Subscriptie simpla: {(city,=,"Bucharest");(temp,>=,10);(wind,<,11)} - In acest caz un subscriber va fi notificat cu toate publicatiile care au o potrivire pozitiva evaluata prin simpla comparatie a campurilor corespondente din subscriptie si publicatie.

Subscriptie complexa: {(city,=,"Bucharest");(avg_temp,>,8.5);(avg_wind,<=,13)} - Campurile "avg_" indica in exemplu un criteriu de medie pe o fereastra de publicatii. Se va considera o dimensiune fixa a ferestrei ce va fi determinata pe baza unui contor de publicatii. Dimensiunea ferestrei va fi data ca parametru de configurare a sistemului (ex. 10 publicatii pe fereastra). Un subscriber va fi notificat printr-un mesaj specific in momentul in care apare o fereastra de publicatii in fluxul generat care va avea o potrivire cu respectivul criteriu. In exemplul dat, cand ultimele 10 publicatii care redau starea meteo din Bucuresti au mediile de temperatura si vant dorite de un subscriber, i se va trimite un mesaj de notificare special de tip "meta-publicatie": {(city,=,"Bucharest");(conditions,=,true)}. Se cere implementarea a cel putin un criteriu de procesare pe fereastra pentru un camp. Criteriul poate fi la alegere (medie, maxim, etc.) iar modul de avans al ferestrei va fi tumbling window (fiecare fereastra va urma distinct in succesiune celei anterioare dupa completarea numarului de publicatii care o compun). Nu se cere tratarea situatiilor de inordine a publicatiilor dintr-o fereastra. 