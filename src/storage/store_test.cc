#include <iostream>
#include <string>
#include "hash_store.hh"

using namespace std;

typedef struct  {
  int number;
} Address;

int main(int argc, char** argv) {
  HashStore<int, Address> *storage = new HashStore<int, Address>();
  Address vals[3];
  vals[0].number = 0;
  vals[1].number = 1;
  vals[2].number = 2;
  
  string jose, manuel, faleiro;
  jose = "jose";
  manuel = "manuel";
  faleiro = "faleiro";

  // Make sure that put works properly. 
  storage->Put(0, &vals[0]);
  storage->Put(1, &vals[1]);
  storage->Put(2, &vals[2]);

  // Make sure that we're able to properly get items from storage. 
  Address *temp;
  temp = storage->Get(0);
  cout << "jose " << temp->number << "\n";
  
  temp = storage->Get(1);
  cout << "manuel " << temp->number << "\n";
  
  temp = storage->Get(2);
  cout << "faleiro " << temp->number << "\n";
  
  return 0;
}
