#include <iostream>
#include <string.h>
#include <vector>
#include <fstream>
#include <stdlib.h>
#include <algorithm>
#include <bits/stdc++.h>

using namespace std;

string path;
vector<string> packages;

void readPath() {
	ifstream indata; 
	string line; 
	int count = 0;
	
	indata.open("path.txt"); 
	
	if(!indata) { 
	    cout << "Error: file could not be opened" << endl;
	    exit(1);
	}
	
	while(getline(indata, line)) {
		count++;
		
		if(count == 1) {
			replace(line.begin(), line.end(), '\\', '/');
			path = line;
			break;
		}
		
	}
	
	indata.close();
}

void readPackages() {
	ifstream indata; 
	string line; 
	
	indata.open("requirements.txt"); 
	
	if(!indata) { 
	    cout << "Error: file could not be opened" << endl;
	    exit(1);
	}
	
	while(getline(indata, line)) { 
	    packages.push_back(line);
	}
  	 
	indata.close();
}

void installPackages() {
	
	for(int i = 0; i < packages.size(); i++) {
		string curPackage = packages[i];
		string str = "pip install --target=" + path + " " + curPackage;
		const char* command = str.c_str();
		
		system(command);
		cout << "Package " << curPackage << " installed\n";
	}
	
}

int main() {
	readPath();
	readPackages();
	installPackages();

	cout << "\nProcess Complete\n";
	system("pause");
	
	return 0;
}
