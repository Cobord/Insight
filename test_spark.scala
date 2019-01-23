/*val NUM_SAMPLES = 100000
val pi_approx = sc.parallelize(1 to NUM_SAMPLES).filter{_=>
    val x = math.random
    val y = math.random
    x*x + y*y < 1}.count()*4.0/(NUM_SAMPLES)
println("Pi is roughly ${pi_approx}")*/

//val text = sc.textFile("/user/alice.txt")
val text=sc.textFile("hdfs://ec2-54-82-213-27.compute-1.amazonaws.com:9000/user/alice.txt")
//val text=sc.textFile("alice.txt")
val counts = text.flatMap(line=>line.split(" ")).map(word=>(word,1)).reduceByKey(_+_)
val answer = counts.collect()
println(answer)

def piecewise_linear(fOfx : Array[(Double,Double)]):Array[((Double,Double),Double)] = {
    val xs = fOfx map {case (x,y) => x}
    val ys = fOfx map {case (x,y) => y}
    val deltaxs=xs zip xs.tail
    val deltaxs_len=deltaxs map {case (x1,x2)=>x2-x1}
    val deltaf = (ys zip ys.tail) map {case (y1,y2) => y2-y1}
    val slopes = (deltaf zip deltaxs_len) map {case (dy,dx)=>dy/dx}
    (deltaxs zip slopes)
}

def zero_average(signal : Array[((Double,Double),Double)]): Array[((Double,Double),Double)]={
    val ((start_time,_),_)=signal(0)
    val ((_,stop_time),_)=signal.last
    val x_intervals = signal map {case x => x._1}
    val y_values = signal map {case x => x._2}
    val x_intervals_len = x_intervals map {case (x1,x2)=>x2-x1}
    val intgrl = (y_values zip x_intervals_len) map {case (y,dx)=>y*dx}
    val avg=intgrl.sum/(stop_time-start_time)
    signal map {case x => (x._1,x._2-avg)}
}

def scale_down(signal : Array[((Double,Double),Double)]): Array[((Double,Double),Double)]={
    val ((start_time,_),_)=signal(0)
    val ((_,stop_time),_)=signal.last
    val x_intervals =signal map {case x=>x._1}
    val y_values = signal map {case x=>x._2}
    val x_intervals_len = x_intervals map {case (x1,x2)=>x2-x1}
    val intgrl = (y_values zip x_intervals_len) map {case (y,dx)=>y*y*dx}
    val avg_sq=intgrl.sum/(stop_time-start_time)
    signal map {case x => (x._1,x._2/math.sqrt(avg_sq))}
}

def normalized_signal(signal : Array[(Double,Double)]): Array[((Double,Double),Double)]={
    scale_down(zero_average(piecewise_linear(signal)))
}

def cross_corr(signal1:Array[((Double,Double),Double)],signal2:Array[((Double,Double),Double)]): Array[(Double,Double)]={
    //TODO
    Array((1,1),(2,2))
}