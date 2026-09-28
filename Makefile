.PHONY: test replay

test:
	python3 -m unittest discover -s tests -v

replay:
	python3 scripts/replay.py
