PY = .env/bin/python3


init:
	@if [ -e .env ]; then \
  		conda env update --prefix .env --prune; \
	else \
  		conda env create --prefix .env; \
	fi


query:
	@$(PY) -m etha.gateway.main


.PHONY: init query
