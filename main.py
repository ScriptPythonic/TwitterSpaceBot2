from bot import create_app
import schedule
import time 

app = create_app()

if __name__ == '__main__':

    app = create_app()
    app.run(debug=True, port=4000)


   
   
