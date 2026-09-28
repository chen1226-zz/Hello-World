.PHONY: replay test clean

replay:
	python3 replay.py

test:
	python3 -m unittest discover -s tests -t . -v

clean:
	rm -rf __pycache__ tests/__pycache__
