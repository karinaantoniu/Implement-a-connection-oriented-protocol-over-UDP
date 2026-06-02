# Tema 2 PCOM - Protocol Reliable peste UDP

Antoniu Karina-Maria, 321CC

## Conexiunea server-client (Three Way Handshake)
Clientul este cel care trimite primul un segment de control catre server, catre un port fix, 8032, cunoscut din cerinta. Prin acest mesaj (SYN), clientul nu trimite date utile, ci doar anunta intentia de a deschide un nou canal de comunicare. 

Serverul, aflat initial intr-o stare blocanta, receptioneaza segmentul. Pentru a nu bloca portul principal, serverul aloca aleator un port nou pentru aceasta conexiune. Acesta creeaza un socket nou si leaga portul de el. Trimite inapoi un alt segment de cotrol in care include, in payload-ul acestuia, si numarul portului nou creat. Prin acest segment, serverul ii confirma clientului ca operatia de conexiune a fost acceptat si a avut succes. 

Clientul, receptioneaza acest nou pachet, extrage noul port din payload si actualizeaza in structura sockaddr_in destinatia. In final, trimite un ultim segment de control (ACK) direct pe noul port creat in care confirma serverului receptionarea modificarii. Din acest moment, conexiunea este stabilita si comunicarea se va realiza independent pe noul socket.

Detalii despre implementare: Am codificat dupa cum urmeaza, cele 3 variabile specifice conexiunii Three Way Handshake:
* 1 - SYN (Initiaza conexiunea si cere serverului sa deschida un canal de comunicare)
* 2 - SYN_ACK (Confirma primirea cererii si informeaza clientul despre noul port, eliberand astfel portul principal de listen.)
* 3 - ACK (Confirma primirea pachetelor)
* 4 - DATA (Transporta fragmentele efective din fisier (payload-ul))
    
## Mecanismul de receive and send data
In cerinta se doreste implementarea unui sistem de tip "Selective Repeat" unde receiver-ul confirma individual fiecare segment primit, iar sender-ul trimite doar pachetele pentru care timer-ul a expirat. Prin urmare am modificat structura conexiunii si am adaugat urmatoarele variabile esentiale pentru rezolvarea problemei: 

### Pentru sender (client)
1. un buffer din care trimite segmente
1. un buffer pentru pachetele trimise, dar care asteapta sa fie confirmate pentru a le putea sterge complet din memorie
1. un contor al bazei curente pentru retransmisie
1. un alt contor pentru id-ul urmatorului segment de trimis
1. o variabila care memoreaza capacitatea maxima a ferestrei

### Pentru receiver (server)
1. in mod similar, memorez id-ul urmatorului segment
1. un contor pentru dimensiunea maxima a bufferului
1. o variabila pentru baza fereastrei
1. un buffer pentru pachetele primite
1. un buffer pentru pachetele neordonate care ajung la server si pe care trebuie sa le confirm


In sender_handler verific intai retransmisia, adica parcurg pachetele din map, iar daca timpul scurs depaseste limita de timeout, pachetul este retrimis si resetez timer-ul. Apoi, cand primesc un ACK de la server, fereastra gliseaza prin stergerea pachetului din map si incrementez contorul aferent bazei ferestrei. Ulterior, verific ca numarul de pachete trimise si neconfirmate sa fie mai mic decat fereastra admisa. Daca acest lucru este posibil si aplicatia mai are pachete de trimis (adica senderBuffer nu este gol) le trimit catre server si pun o copie a acestora in bufferul meu waitingForACKBuffer pentru cazul in care da timeout si trebuie retrimise. 

In implementarea serverului, pachetele sunt gestionate cu ajutorul a doua buffere: o coada receiverBuffer pentru pachetele ordonate corect si pregatite pentru aplicatie si o coada de prioritati folosit ca spatiu pentru pachetele primite, dar neprocesate inca. Cand serverul primeste un segment il confirma printr-un pachet de tip ACK clientului. Daca pachetul primit are fix numarul de secventa asteptat, atunci este pus direct in bufferul aplicatiei si verific daca mai am pachete cu numar de secventa consecutiv cu cel primit in outOfOrderBuffer. Daca in outOfOrder gasesc acele pachete, le pun extrag si pe ele din coada si le pun in serverul aplicatiei. Daca in schimb, primesc un pachet cu un seq_nr diferit de cel asteptat il pun in bufferul de outOfOrder, pentru ca a este posibil sa fii urmat o ruta mai rapida si sa fi ajuns mai devreme decat cel asteptat sau ca cel asteptat sa trebuiasca retrimis din cauza timeout-ului.

Pentru simplitatea gestionarii stergerii unui pachet din buffer, am decis sa folosesc o coada si sa memorez variabile de tip Packet care contin pe langa datele efective (payload-ul) si un contor pentru dimensiune, pentru numarul secventei si unul pentru timpul la care a fost trimis (util pentru a calcula timeout-ul). Pentru pachetele ce ajung la server intr-o ordine haotica, am decis sa folosesc un priority queue care sa fie sortat dupa numarul secventei. Astfel incat, desi ajung haotic la destinatie sa pot sa le gasesc usor si sa le confirm.

### Problema cu coada
Initial, aceasta solutie mergea, pachetele se trimiteau local si scoteau un timp decent. Ruland checker-ul in schimb, nici un test nu trecea, timpul era mult prea mare fata de cerinte. Modificand parametrii si ruland de mai multe ori, am observat ca nu era o diferenta majora intre rulari. Motiv pentru care, analizand codul am hotarat ca ar fi mai bine sa modific structura bufferului pentru pachetele ce asteapta ACK-ul. Cautarea in coada, si adaugarea lor intr-o coada suplimentara manca foarte mult timp si spatiu, si trebuie ca de fiecare data cand cautam pachete sa parcurg toata coada de la cap la coada si sa o pun pe cea actualizata in locul celei vechi. Descoperind acest aspect, am hotarat ca ar fi mai bine sa ma folosesc de un map, care a redus cautarea si stergerea la o complexitate logaritmica. Un pachet este eliminat instant din buffer, iar baza ferestrei gliseaza odata cu citirea primului element din map.
