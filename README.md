## Inspiration for this project
Keshif: https://github.com/adilyalcin/Keshif

## Steps to run this code
1. Install elasticsearch: http://www.elasticsearch.org/	(some troubleshooting tips found here: https://github.com/CenterForOpenScience/osf#e\
lasticsearch), and make sure it is running
2. Install the requirements.txt with ``` pip install -U	     requirements.txt ```
3. (optional but highly reccomended) Create a new virtualenv for managing bokeh deps, and activate it
4. Seed elasticsearch with random data by running ``` invoke seed_elastic ``` (by default this creates 100000 documents, you can change this by passing ``` --size=[SIZE] ``` to invoke
5. Install bokeh
  1. ``` git clone git@github.com:ContinuumIO/bokeh.git	&& cd bokeh```
  2. ``` [PATH_TO_YOUR_VIRTUAL_ENV]/bin/python setup.py	install	--build_js ```
6. From within the bokeh directory, run the bokeh server: ``` ./bokeh-server ```
7. From within this directory (and with your virtualenv sourced), run ``` python plot.py ```