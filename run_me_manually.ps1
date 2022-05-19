$t= Read-Host "Please input the trade date, format = [YYYYMMDD]"
python 3_download_security_id_md_fm.py $t htzq12157 247260 T
python 8_copy_to_portal.py $t
Pause
