import sys


if __name__ == '__main__':
    
    file1 = open(sys.argv[1])
    file2 = open(sys.argv[2])
    
    ctr = 1
    
    while True:
        line1 = file1.readline()
        line2 = file2.readline()
        if (not line1) or (not line2):
            break
        if line1 != line2:
            break
        ctr += 1
        
    print(f"Diff at line {ctr}")
        
        