# Live-Hack: Analyzing 7 years of Buzzwords (at Scale)

* Session Abstract: https://www.berlinbuzzwords.de/session/live-hack-analyzing-7-years-buzzwords-scale
* Slides: https://speakerdeck.com/ctavan/live-hack-analyzing-7-years-of-buzzwords-at-scale

## Scraping

Using http://scrapy.org/ on Ubuntu:

```
sudo apt-get update
sudo apt-get install -y python-virtualenv python-dev python-pip libxml2-dev libxslt1-dev zlib1g-dev libffi-dev libssl-dev

virtualenv venv
. venv/bin/activate
pip install Scrapy
pip install boto

cat <<EOF > ~/.botocfg
AWSAccessKeyId=YOUR_AWS_ACCESS_KEY
AWSSecretKey=YOUR_AWS_SECRET_KEY
EOF

export AWS_CREDENTIAL_FILE=~/.botocfg

# Run scrapy like this:
time scrapy runspider bbuzz_spider.py -s LOG_LEVEL=INFO -o s3://YOUR_BUCKET/all-years.jsonlines
```

## Analysis

Check bbuzz2016-backup.* in this repository.
