setup:
	pip install -r requirements.txt

all:
	black duneapi.py
	flake8  duneapi.py
