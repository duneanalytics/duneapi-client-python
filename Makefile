setup:
	pip install -r requirements.txt

all:
	black -l 120 duneapi.py
	flake8 --max-line-length 120 duneapi.py
