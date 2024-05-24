# def solution(area):
#     nearest_val = area ** (1/2)
#     nearest_val = int(nearest_val) * int(nearest_val)
#     balance = area - nearest_val
#     return [nearest_val] + solution(balance)

def get_biggest_square(max_number):
    n = 1
    while (n * n < max_number + 1):
        n = n + 1
    return n - 1


def solution(area):
    if (area > 1000000 or area < 1):
        raise ValueError('Area is outside of bounds')
    array = []
    while (area != 0):
        res = get_biggest_square(area)
        array.append(res * res)
        area -= res * res

    return array


output = solution(15324)
print(output)
