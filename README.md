# gp_crawler

In order to set up an EC2 to work with the crawler you need to run the following commands:

>sudo apt-get update
>wget https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb
>sudo dpkg -i --force-depends google-chrome-stable_current_amd64.deb
>sudo apt-get install -f
>sudo apt-get -y install python3-pip
>sudo pip3 install --upgrade pip
>git clone https://github.com/pszaflarski/gp_crawler.git
when in the gp_crawler folder:
>sudo pip3 install -r requirements.txt

you will then need to copy your version of _credentials.json_ to the gp_crawler folder
