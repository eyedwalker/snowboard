.PHONY: build-SnowflakeActionGroupFunction

build-SnowflakeActionGroupFunction:
	pip install -r lambda/requirements.txt \
		--platform manylinux2014_x86_64 \
		--implementation cp \
		--python-version 3.11 \
		--only-binary=:all: \
		--upgrade \
		-t "$(ARTIFACTS_DIR)/"
	cp lambda/handler.py "$(ARTIFACTS_DIR)/"
	cp -r src "$(ARTIFACTS_DIR)/"
