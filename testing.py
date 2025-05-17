
inp = '''5 3
3 1 3 5
2 2 4
5 1 2 3 4 5'''.split('\n')

n, m = list(map(int,inp[0].split(' ')))
def create_first():
    data = {i:[] for i in range(1,n+1)}
    for line in inp[1:]:
        line_list = list(map(int, line.split(' ')))
        for i in range(len(line_list)-1):
            data[line_list[i]].append(line_list[i+1])
            data[line_list[::-1][i]].append(line_list[::-1][i+1])
    new_data = {k:list(set(v)) for k,v in data.items()}
    result = []
    for i, (k,v) in enumerate(data.items()):
        v = set(v)
        answer_line = [0 for _ in range(n)]
        for vv in v:
            answer_line[vv-1]=1
        answer_line[i]=0
        result.append(answer_line)
    return result

def create_second():
    result = []
    for i in range(n):
        line = [1 for _ in range(n)]
        line[i]=0
        result.append(line)
    return result

def print_fun(matrix):
    for line in matrix:
        print(' '.join(list(map(str, line))))

print_fun(create_first())
print_fun(create_second())