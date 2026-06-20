#runningnya gini :
cp .env.example .env
docker compose up --build -d
docker compose ps

#tes endpoint check :
http://localhost:8001/health
http://localhost:8002/health
http://localhost:8003/health

#jalankan demo.html :
python -m http.server 5500
http://localhost:5500/anu/demo.html (copy yang ini lgsg)

#lihat visual rabbitmq :
http://localhost:15672

#lihat log :
docker compose logs -f integration-service

#stop sistem :
docker compose down

#reset sistem :
docker compose down -v
docker compose up --build -d (build ulang)

#cara jalain dokumentasinya api nya :
switch cd dl jangan masuk ke cd demo.html
python -m http.server 5500
http://localhost:5500/api-docs.html
