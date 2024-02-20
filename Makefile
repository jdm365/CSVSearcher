all:
	python -m pip install .

clean:
	rm -r build dist *.egg-info .cache
