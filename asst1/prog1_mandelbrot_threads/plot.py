import matplotlib.pyplot as plt

threads, speedup = [], []
with open("partb.csv", mode="r") as f:
    while True:
        try:
            t, s = f.readline().split(",")
            threads.append(int(t))
            speedup.append(float(s))
        except:
            break

plt.plot(threads, speedup, marker='o')
plt.xlabel("Number of Threads")
plt.ylabel("Speedup vs Serial")
plt.title("Mandelbrot Speedup (View 1)")
plt.grid(True)

plt.savefig("partb.png")
plt.show()
