wget https://dbs.uni-leipzig.de/files/datasets/saeedi/musicbrainz-20000-A01.csv.zip;
wget https://dbs.uni-leipzig.de/files/datasets/saeedi/musicbrainz-2000-A01.csv.dapo;
unzip musicbrainz-20000-A01.csv.zip; rm musicbrainz-20000-A01.csv.zip;
mv musicbrainz-20000-A01.csv tests/mb.csv;
mv musicbrainz-20000-A01.csv.dapo tests/mb_small.csv;
