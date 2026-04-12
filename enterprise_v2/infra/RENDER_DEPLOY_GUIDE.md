# Render Deploy Guide - Enterprise V2

## Obiettivo
Pubblicare enterprise_v2 come servizi separati senza toccare il sito attuale.

## Servizio 1 - Backend
- Root directory: enterprise_v2/backend
- Runtime: Docker
- Dockerfile path: enterprise_v2/backend/Dockerfile
- Start: definito nel Dockerfile
- Health check path: /health

## Servizio 2 - Frontend
- Root directory: enterprise_v2/frontend
- Runtime: Docker
- Dockerfile path: enterprise_v2/frontend/Dockerfile

## Variabili da impostare
### Frontend
- VITE_API_BASE_URL=https://URL-BACKEND-ENTERPRISE-V2.onrender.com

## Test minimi post deploy
1. Aprire backend /health
2. Aprire frontend
3. Testare Upload & Inspect
4. Aprire Saving Dashboard
5. Aprire Risorse
6. Aprire Tempi Attraversamento
7. Aprire Non Conformità

## Regola
Non sostituire il servizio live attuale finché enterprise_v2 non è verificata.
