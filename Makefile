########################################################################################################################
# Project setup
########################################################################################################################

init_env : init_virtualenv load_direnv install 
	@echo "✅ Environment initialized and ready to use 🔥"

init_virtualenv :
	@echo "Initializing environment ..."
	@if pyenv virtualenvs | grep -q 'finboard'; then \
		echo "Virtualenv 'finboard' already exists"; \
	else \
		echo "Virtualenv 'finboard' does not exist"; \
		echo "Creating virtualenv 'finboard' ..."; \
		pyenv virtualenv 3.10.12 finboard; \
	fi

	@pyenv local finboard
	@echo "✅ Virtualenv 'finboard' activated"

load_direnv:
	@echo "Loading direnv ..."
	@direnv allow
	@echo "✅ Direnv loaded"

# precommit_install:
# 	@echo "Installing pre-commit hooks ..."
# 	@pre-commit install
# 	@echo "✅ Pre-commit hooks installed"

install :
	@echo "Installing dependencies ..."
	@pip install --upgrade -q pip
	@pip install -q -r requirements.txt
	@echo "✅ Dependencies installed"

########################################################################################################################
# Test api
########################################################################################################################

.PHONY:
# start test api code 
testapi:
	python test_api_carbonapi.py


push_to_GCR:
	python connect_course_to_GCP.py


toy_api:
	python toy_api.py