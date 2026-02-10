# Setup para o Core como pacote instalável


from setuptools import setup, find_packages
from setuptools.command.install import install
import os

class CustomInstallCommand(install):
    '''Atua como um start no pip install'''
    def run(self):                        
        install.run(self)
        os.system("python -m SQLManager._model._model_update")

setup(
    name="SQLManager",
    version="2.4.0",
    description="Sistema para gerenciamento de banco de dados e validações",
    author="Rojemac",
    author_email="nicolas.santos@avalontecnologia.com.br",
    url="https://github.com/nickzsd/SQLManager",
    packages=find_packages(include=["SQLManager", "SQLManager.*"]),
    include_package_data=True,
    package_data={
        "SQLManager": [
            "_model/*.py",
            "EDTs/*.py",
            "enum/*.py",
            "tables/*.py",
        ]
    },
    python_requires=">=3.8",
    install_requires=[
        "pyodbc>=4.0.0",
    ],
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
    ],
    cmdclass={
        'install': CustomInstallCommand,
    },
)
