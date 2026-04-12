# Legacy Context-Aware Upload Pack

## Obiettivo
Interpretare lo stesso file in base alla box di caricamento.

## Regole
- box Saving -> analisi saving / ordini
- box Risorse -> analisi risorse / operatività
- box Tempi -> analisi tempi
- box NC -> analisi non conformità

## Nota
Il backend riceve il contesto di dominio attraverso l'endpoint scelto e prova a interpretare il file coerentemente con quel dominio.
