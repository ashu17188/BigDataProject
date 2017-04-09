package org.ashu.spark


object BasicSaveProtoBuf {
/*    def main(args: Array[String]) {
      val master = "local"
      val outputFile = "/home/hduser/BigData_Workspace/BigDataProject/resources/data/placesOutput2.proto"
      val sc = new SparkContext(master, "BasicSaveProtoBuf", System.getenv("SPARK_HOME"))
      val conf = new Configuration()
      LzoProtobufBlockOutputFormat.setClassConf(classOf[protobuf.Venue], conf);
      val dnaLounge = Places.Venue.newBuilder()
      dnaLounge.setId(1);
      dnaLounge.setName("DNA Lounge")
      dnaLounge.setType(Places.Venue.VenueType.CLUB)
      val data = sc.parallelize(List(dnaLounge.build()))
      val outputData = data.map{ pb =>
        val protoWritable = ProtobufWritable.newInstance(classOf[Places.Venue]);
        protoWritable.set(pb)
        (null, protoWritable)
      }
      outputData.saveAsNewAPIHadoopFile(outputFile, classOf[Text], classOf[ProtobufWritable[Places.Venue]],
        classOf[LzoProtobufBlockOutputFormat[ProtobufWritable[Places.Venue]]], conf)
    }
*/}
