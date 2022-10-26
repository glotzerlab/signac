string1 = "Welcome to python 3.9"
print("Original String 1 : ", string1)

# prefix exists
result = string1.removeprefix("Welcome")
print("New string : ", result)

string2 = "Welcome buddy"
print("Original String 2 : ", string2)

# prefix doesn't exist
result = string2.removeprefix("buddy")
print("New string : ", result)
