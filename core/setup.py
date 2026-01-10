# Setup para o Core como pacote instalável

from setuptools import setup, find_packages

setup(
    name="sqlmanager-core",
    version="1.0.0",
    description="Sistema reutilizável para gerenciamento de banco de dados e validações",
    author="Nicolas Santos",
    author_email="nicolas.santos@avalontecnologia.com.br",
    url="https://github.com/seu-usuario/sqlmanager-core",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
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
)
