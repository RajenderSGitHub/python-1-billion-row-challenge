
string = "Abidjan,15.7"

filename = "measurements_small.csv"

result = {}
with open(filename, "r") as f:
    next(f)
    list = []
    for line in f:
        name, temp = line.split(',')
        temp = round(float(temp),2)
        print(name, temp)
        if name not in result: 
            result[name] = [temp, temp, temp, 1]
        else:
            if temp < result[name][0]:
                result[name][0] = temp
            if temp > result[name][1]:
                result[name][1] = temp
            result[name][2] += temp
            result[name][3] += 1

for key, value in result.items(): 
    print(f"Average, minimum and maximum for {key} are {round(result[key][2]/result[key][3],2)}  ")