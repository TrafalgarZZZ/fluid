import json
import glob
import os

FLUID_RUNTIME_MNT = os.environ.get("MOUNT_POINT")
FLUID_MOUNT_OPT_DIR = "/etc/fluid/mount-opts"
FLUID_CONFIG_FILE = "/etc/fluid/config/config.json"
SUPERVISORD_SETTING_DIR = "/etc/supervisor/conf.d"
SUPERVISORD_SETTING_TEMPLATE = """[program:{name}]
command=mount-helper.sh mount {mount_src} {mount_target} {mount_opt_file}
stdout_logfile=/var/logs/fluid/{name}.out
stderr_logfile=/var/logs/fluid/{name}.err
autorestart=true
startretries=9999"""

def prepare_dirs():
    os.makedirs(SUPERVISORD_SETTING_DIR, exist_ok=True)
    os.makedirs("/var/logs/fluid", exist_ok=True)
    os.makedirs(FLUID_MOUNT_OPT_DIR, exist_ok=True)

def write_mount_opts(mount_opts, opt_file):
    with open(opt_file, "w") as f:
        f.write(json.dumps(mount_opts))

def reconcile_supervisord_settings():
    rawStr = ""
    with open(FLUID_CONFIG_FILE, "r") as f:
        rawStr = f.readlines()

    print(f"{FLUID_CONFIG_FILE}: {rawStr[0]}") # config.json only have one line in json format

    setting_files = glob.glob(os.path.join(SUPERVISORD_SETTING_DIR, "*.conf"))

    # obj["mounts"] is like [{"mountPoint": "s3://mybucket", "name": "mybucket", "path": "/mybucket", "options":{...}}, {"mountPoint": "s3://mybucket2", "name": "mybucket2", "path": "/mybucket2", "options":{...}}]
    obj = json.loads(rawStr[0])
    expected_mounts = [mount["name"] for mount in obj["mounts"]]
    current_mounts = [os.path.basename(file).removesuffix(".conf") for file in setting_files]

    need_mount = list(set(expected_mounts).difference(set(current_mounts)))
    need_unmount = list(set(current_mounts).difference(set(expected_mounts)))
    print(f"need mount: {need_mount}, need umount: {need_unmount}")

    for name in need_unmount:
        setting_file = os.path.join(SUPERVISORD_SETTING_DIR, f"{name}.conf")
        if os.path.isfile(setting_file):
            os.remove(setting_file)
            print(f"Mount \"{name}\"'s settings has been removed.")

    
    mount_info_dict = {mount["name"]: mount for mount in obj["mounts"]}
    for name in need_mount:
        if name not in mount_info_dict:
            print(f"WARNING: mount \"{name}\" is not found in {FLUID_CONFIG_FILE}.")
            continue
        mount_info = mount_info_dict[name]
        mount_src = mount_info["mountPoint"]
        mount_dir_name = name
        if "path" in mount_info:
            if mount_info["path"] != "/":
                mount_dir_name = mount_info["path"].lstrip("/")
            else:
                print(f"WARNING: mounting \"{name}\" at \"/\" is not allowed, fall back to mount at \"/{name}\"")
        mount_target = os.path.join(FLUID_RUNTIME_MNT, mount_dir_name)
        mount_opt_file = os.path.join(FLUID_MOUNT_OPT_DIR, f"{name}.opts")
        write_mount_opts(mount_info["options"], mount_opt_file)

        setting_file = os.path.join(SUPERVISORD_SETTING_DIR, f"{name}.conf")
        with open(setting_file, 'w') as f:
            f.write(SUPERVISORD_SETTING_TEMPLATE.format(name=name, mount_src=mount_src, mount_target=mount_target, mount_opt_file=mount_opt_file))

        print(f"Mount \"{name}\"'s setting is successfully written to {setting_file}")

if __name__=="__main__":
    prepare_dirs()
    reconcile_supervisord_settings()