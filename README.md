# Install instructions 
- Create a new virtualenv
  - Make sure `python3 --version` returns 3.9.4
  - `pip install virtualenv`
  - `python3 -m venv env`
- Run `source env/bin/activate`
- Install the packages using `pip install -r requirements.txt` 

# Before running code 
- Run `source env/bin/activate`

# When making changes in packages
- Run `pip install XX`
- Run `pip freeze > requirements.txt` 

# Compile and run docker
- Compile docker: `docker build -t alpha-defi-test .`
- Run docker `docker run -t alpha-defi-test`