atom:
	pm2 start ./daemon/atom.sh --name atom --silent --log --cron-restart="1 0 * * *"
	pm2 start ./daemon/download.sh --name atom --silent --log --cron-restart="1 0 * * *"
	pm2 start ./daemon/web.sh --name atom --silent --log --cron-restart="1 0 * * *"