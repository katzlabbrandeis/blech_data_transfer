"""
Use fabric to run comamnds on remote via ssh
"""

from fabric import Connection
import os

# cxn_name = 'bumblebaby'
# c = Connection(host = cxn_name, user = 'abuzarmahmood')
# 
# result = c.run(
#         """
#         source ~/anaconda3/bin/activate && 
#         conda activate blech_clust && 
#         which python
#         """, 
#         pty=True)

##############################

cxn_name = 'xsede_autosort'
c = Connection(host = cxn_name, user = 'exouser')
c.run('echo $USER')
c.run('echo $HOME')

# install conda and blech_clust
# c.run('wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh')
# c.run('bash Miniconda3-latest-Linux-x86_64.sh')

############################################################
# Install miniconda
c.run('mkdir -p ~/miniconda3')
c.run('wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh -O ~/miniconda3/miniconda.sh')
c.run('bash ~/miniconda3/miniconda.sh -b -u -p ~/miniconda3')
c.run('rm -rf ~/miniconda3/miniconda.sh')
c.run('~/miniconda3/bin/conda init bash')
c.run('. ~/.bashrc')
############################################################

# c.run("""
#       source ~/miniconda3/bin/activate &&
#       conda info --envs
#       """,
#       pty=True)
#       )

# def run_cmd_conda(cmd, c=c):
#     c.run(f"""
#           source ~/miniconda3/bin/activate &&
#           {cmd}
#           """,
#           pty=True)
# 
# def run_cmd_conda_seq(cmds, c=c, env_name='blech_clust'):
#     """
#     Run commands given as list of strings
#     Only executes next command if previous one is successful
#     """
#     n_cmds = len(cmds)
#     for i, cmd in enumerate(cmds):
#         print(f"Running command {i+1}/{n_cmds}: {cmd}")
#         out = c.run(f"""
#               source ~/miniconda3/bin/activate &&
#               {cmd}
#               """,
#               pty=True)
#         if out.failed:
#             print(f"Command failed: {cmd}")
#             print('Exiting...')
#             break


############################################################
blech_install_cmd_tuples = [
        ('base', '~/Desktop', 'git clone https://github.com/katzlabbrandeis/blech_clust.git'),
        ('base', '~/Desktop', 'conda update -n base conda'),
        ('base', '~/Desktop/blech_clust/requirements', 'conda clean --all -y'),
        ('base', '~/Desktop/blech_clust/requirements', 'conda create --name blech_clust python=3.8.13 -y'),
        ('blech_clust',  '~/Desktop/blech_clust/requirements', 'conda install -c conda-forge -y --file conda_requirements_base.txt'),
        ('blech_clust',  '~/Desktop/blech_clust/requirements', 'bash install_gnu_parallel.sh'),
        ('blech_clust',  '~/Desktop/blech_clust/requirements', 'pip install -r pip_requirements_base.txt'),
        ('blech_clust',  '~/Desktop/blech_clust/requirements', 'bash patch_dependencies.sh')
        ('base', '~/Desktop/blech_clust', 'cp params/_templates/* params/')
        ]
neurecommend_install_cmd_tuples = [
        ('base', '~/Desktop', 'git clone https://github.com/abuzarmahmood/neuRecommend.git'),
        ('blech_clust', '~/Desktop/neuRecommend', 'pip install -r requirements.txt')
        ]

def run_cmd_tuple(cmd_tuple, c=c, pty=True):
    env_name, dir_name, cmd = cmd_tuple
    out = c.run(f"""
          cd {dir_name} &&
          source ~/miniconda3/bin/activate &&
          conda activate {env_name} &&
          {cmd}
          """,
          pty=pty)
    return out

# Install blech_clust
for i, cmd_tuple in enumerate(blech_install_cmd_tuples):
    print(f"Running command {i+1}/{len(blech_install_cmd_tuples)}: {cmd_tuple}")
    out = run_cmd_tuple(cmd_tuple)
    if out.failed:
        print(f"Command failed: {cmd_tuple}")
        print('Exiting...')
        break

# Install neuRecommend
for i, cmd_tuple in enumerate(neurecommend_install_cmd_tuples):
    print(f"Running command {i+1}/{len(neurecommend_install_cmd_tuples)}: {cmd_tuple}")
    out = run_cmd_tuple(cmd_tuple)
    if out.failed:
        print(f"Command failed: {cmd_tuple}")
        print('Exiting...')
        break

############################################################
# Copy files from local to remote
DIR='/media/bigdata/Abuzar_Data/ORX15_spont_230529_095725'
basename = os.path.basename(DIR)
REMOTE_DIR = '~/Desktop/'

# c.run(f'mkdir -p ~/Desktop/{basename}')
# c.put(DIR, f'~/Desktop/{basename}')
import patchwork.transfers
patchwork.transfers.rsync(
        c,
        source = DIR,
        target = REMOTE_DIR,
        rsync_opts = '-avP',
        )

############################################################
# Run autosort
remote_dir_path = 'pipeline_testing/test_data_handling/test_data/KM45_5tastes_210620_113227_new'
clean_slate_tuple = (
        'blech_clust', 
        '~/Desktop/blech_clust', 
        f'python blech_clean_slate.py {remote_dir_path}'
        )
autosort_cmd_tuple = (
        'blech_clust', 
        '~/Desktop/blech_clust', 
        f'bash blech_autosort.sh {remote_dir_path}'
        )

out = run_cmd_tuple(clean_slate_tuple)
# Add code here to confirm *.info and *electrode_layout.csv files are present
out = run_cmd_tuple(autosort_cmd_tuple)
