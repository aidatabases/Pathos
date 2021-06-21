#!/bin/sh

python3 hello.py

# take inputs from the user
echo "Which Function would you like to call ?"
echo "Press 1 to call config Function"
echo "Press 2 to call 'Load Schema' Function"
echo "Press 3 to call 'Load tables' Function"
echo "Press 4 to call 'Generate Transaction Table' Function"
echo "Press 5 to call 'Generate Summary Table' Function"


read a

# check
if [ $a == 1 ]
then
  python3 hello.py
elif [ $a == 2 ]
then
  echo "It's even."
else
  echo "It's odd."
fi

echo "Come Again."
