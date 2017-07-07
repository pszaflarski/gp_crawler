import psutil
import subprocess
import time
import libtmux


def start_crawl(tmux_server):
    session = tmux_server.new_session(session_name="work", attach=False)
    pane = session.attached_pane
    pane.send_keys("sudo python3 start_multiprocess_script.py",enter=True)

def reset_crawl(tmux_server):
    session = tmux_server.find_where({"session_name":"work"})
    session.kill_session()
    start_crawl(tmux_server)


if __name__ == '__main__':

    max_percent = 75
    min_free = 500000000
    wait_step = 3

    server = libtmux.Server()
    start_crawl(server)

    while True:

        free = psutil.virtual_memory().free
        percent = psutil.virtual_memory().percent

        print("free memory:",free,"-",100 - percent,"%")

        if free < min_free or percent > max_percent:
            print("not enough free memory, stopping process and restarting...")
            reset_crawl(server)

        time.sleep(wait_step)

