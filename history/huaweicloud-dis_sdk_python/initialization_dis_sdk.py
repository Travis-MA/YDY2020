#-*-coding:utf-8-*-

from src.com.dis.models.base_model import IS_PYTHON2

if IS_PYTHON2:
    from ConfigParser import ConfigParser
else:
    from configparser import ConfigParser

fp='conf.ini'
conf=ConfigParser()
conf.read(fp)

try:
    # Write configuration file
    conf.remove_section("Section1")
    conf.add_section('Section1')
    conf.set('Section1', 'projectid', '** your projectid **')
    conf.set('Section1', 'ak', '*** Provide your Access Key ***')
    conf.set('Section1', 'sk', '*** Provide your Secret Key ***')
    conf.set('Section1', 'region', '** region **')
    conf.set('Section1', 'endpoint', '** {ip}:{port} ***')
    with open(fp, 'w') as f:
        conf.write(f)
except Exception as ex:
    print(str(ex))




