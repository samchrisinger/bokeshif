from time import sleep
from webbrowser import open_new_tab
from requests.exceptions import ConnectionError

from bokeshif import Bokeshif

app = Bokeshif()

if __name__ == "__main__":
    open_new_tab(app.link)
    print app.link
    try:
        while True:
            app.reload()
            sleep(0.5)
    except KeyboardInterrupt:
        print()
    except ConnectionError:
        print("Connection to bokeh-server was terminated")
