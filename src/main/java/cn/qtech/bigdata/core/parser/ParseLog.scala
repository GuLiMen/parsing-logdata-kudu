package cn.qtech.bigdata.core.parser

import java.util
import java.util.concurrent.atomic.AtomicInteger

import cn.qtech.bigdata.comm.AppConstants._
import cn.qtech.bigdata.model.{AtypiaProcess, RejectRateModel}
import com.alibaba.fastjson.JSONObject
import org.slf4j.LoggerFactory

import scala.util.control.Breaks

class ParseLog {

  private final val LOG = LoggerFactory.getLogger("ParserLog")
  val table_TH = AALOG_SINK_IMPALA_SCHEMA + "." + AALOG_SINK_IMPALA_TABLE_TH
  val table_GC = AALOG_SINK_IMPALA_SCHEMA + "." + AALOG_SINK_IMPALA_TABLE_GC
  val table_CB = AALOG_SINK_IMPALA_SCHEMA + "." + AALOG_SINK_IMPALA_TABLE_CB
  val sidCnt = new AtomicInteger(1)
  var Init_fail_cnt = new AtomicInteger(1)
  var ReInit_fail_cnt = new AtomicInteger(1)
  var AA3_fail_cnt = new AtomicInteger(1)

  // var MTF_Check_fail_cnt = new AtomicInteger(1)
  var MTF_Check1_fail_cnt = new AtomicInteger(1)
  var MTF_Check2_fail_cnt = new AtomicInteger(1)

  // var Chart_Alignment_fail_cnt = new AtomicInteger(1)
  var Chart_Alignment1_fail_cnt = new AtomicInteger(1)
  var Chart_Alignment2_fail_cnt = new AtomicInteger(1)

  var Epoxy_Inspection_fail_cnt = new AtomicInteger(1)
  var SensorID_Check_fail_cnt = new AtomicInteger(1)

  var VCM_init_fail_cnt = new AtomicInteger(1)
  var VCM_init1_fail_cnt = new AtomicInteger(1)
  var VCM_init2_fail_cnt = new AtomicInteger(1)
  var VCM_init3_fail_cnt = new AtomicInteger(1)

  var VCM_PowerOn_fail_cnt = new AtomicInteger(1)
  var VCM_PowerOn1_fail_cnt = new AtomicInteger(1)
  var VCM_PowerOn2_fail_cnt = new AtomicInteger(1)
  var VCM_PowerOn3_fail_cnt = new AtomicInteger(1)

  var VCM_Hall_fail_cnt = new AtomicInteger(1)
  var VCM_Hall1_fail_cnt = new AtomicInteger(1)
  var VCM_Hall2_fail_cnt = new AtomicInteger(1)

  var VCM_Move_fail_cnt = new AtomicInteger(1)
  var VCM_Move1_fail_cnt = new AtomicInteger(1)
  var VCM_Move2_fail_cnt = new AtomicInteger(1)

  var VCM_OIS_Init_fail_cnt = new AtomicInteger(1)
  var VCM_OIS_Init1_fail_cnt = new AtomicInteger(1)
  var VCM_OIS_Init2_fail_cnt = new AtomicInteger(1)

  var INIT_Check_fail_cnt = new AtomicInteger(1)
  var Blemish_Defect_fail_cnt = new AtomicInteger(1)
  var Lightpanel_OC_fail_cnt = new AtomicInteger(1)
  var UV_MTF_Check2_fail_cnt = new AtomicInteger(1)
  var SaveOC_fail_cnt = new AtomicInteger(1)
  var Y_level_fail_cnt = new AtomicInteger(1)

  def parseData(x: Array[String], atypiaProcess: AtypiaProcess, resultReadFile: String) = {
    val rejectRateMap = new JSONObject()
    val upperx1 = x(1).toUpperCase().trim
    //val upperx2 = x(2).toUpperCase().trim
    val foramtx2 = x(2).toUpperCase().replaceAll("\\s+|-|_", "")

    if (("SID".equals(foramtx2) || "SenserID".toUpperCase().equals(foramtx2) || "SensorID".toUpperCase().equals(foramtx2)
      || "RecordSensorID".toUpperCase().equals(foramtx2) || "ReadSensorID".toUpperCase().equals(foramtx2)) && x(3) != null && x.size == 5) {
      val upper3 = x(3).toUpperCase()
      if ("RESULT".equals(upper3)) {
        rejectRateMap.put(SID_CNT, sidCnt.getAndIncrement())
        rejectRateMap.put("SID", x(4))
      }
    }
    else if ("Station".toUpperCase().equals(upperx1)) {
      rejectRateMap.put(upperx1, x(2))
    }
    else if ("Onload".toUpperCase().equals(upperx1)) {
      rejectRateMap.put("Onload_TIME", x(3))
    } else if ("Init".toUpperCase().equals(foramtx2) || "init2".toUpperCase().equals(foramtx2) && x(3) != null) {
      if (x.size == 4) {
        if (x(3).toUpperCase().equals("FAIL")) {
          rejectRateMap.put(INIT_FAIL_CNT, Init_fail_cnt.getAndIncrement())
          rejectRateMap.put(SENSOR_THROW_TIGGER, "Init")
        }
      }
    }
    // "Main_Test".toUpperCase().equals(upperx1)
    else if ("Main_Test".toUpperCase().equals(upperx1) && x.size > 3) {
      if ("MainAAStart".toUpperCase().equals(foramtx2)) {
        rejectRateMap.put("MAIN_AA_START", x(3))
      } else if (x.size > 6) {
        // rejectRateMap.put(upperx1.concat("_LIST"), ArrayUtils.toString(x).replaceAll("\\{|}|TESTLIST,Main_Test,", "").replace(",", "+"))
      } else if ("SensorIDCheck".toUpperCase.equals(foramtx2) && x(3) != null) {
        if (x.size == 4) {
          if (x(3).toUpperCase().equals("FAIL")) {
            rejectRateMap.put(SENSORID_CHECK_FAIL_CNT, SensorID_Check_fail_cnt.getAndIncrement())
            rejectRateMap.put(SENSOR_THROW_TIGGER, "SensorID_Check")
          }
        }
      }
      else if ("AA3".equals(foramtx2) && x(3) != null) {
        if (x.size == 4) {
          if (x(3).toUpperCase().equals("FAIL")) {
            rejectRateMap.put(AA3_FAIL_CNT, AA3_fail_cnt.getAndIncrement())
            rejectRateMap.put(SENSOR_THROW_TIGGER, "AA3")
          }
        }
      } else if (foramtx2.equals("Ylevel".toUpperCase) && x(3) != null) {
        if (x.size == 4) {
          if (x(3).toUpperCase().equals("FAIL")) {
            rejectRateMap.put(Y_level_FAIL_CNT, Y_level_fail_cnt.getAndIncrement())
            rejectRateMap.put(SENSOR_THROW_TIGGER, "Y_level")
          }
        }
      }
      else if ((foramtx2.equals("BlemishDefect".toUpperCase) || foramtx2.equals("LBlemish".toUpperCase) || foramtx2.equals("Blemish".toUpperCase) || foramtx2.equals("LBlemish1".toUpperCase)
        || foramtx2.equals("Blemish1".toUpperCase)) && x(3) != null) {
        if (x.size == 4) {
          if (x(3).toUpperCase().equals("FAIL")) {
            rejectRateMap.put(BLEMISH_DEFECT_FAIL_CNT, Blemish_Defect_fail_cnt.getAndIncrement())
            rejectRateMap.put(SENSOR_THROW_TIGGER, "Blemish")
          }
        }
      }
      else if (("initre".toUpperCase.equals(foramtx2) || "reinit".toUpperCase.equals(foramtx2)) && x(3) != null) {
        if (x.size == 4) {
          if (x(3).toUpperCase().equals("FAIL")) {
            rejectRateMap.put(REINIT_FAIL_CNT, ReInit_fail_cnt.getAndIncrement())
            rejectRateMap.put(SENSOR_THROW_TIGGER, "reinit")
          }
        }
      }
      else if ("VCMInit".toUpperCase.equals(foramtx2) && x(3) != null) {
        atypiaProcess.setVCM_init(true)
        if (x.size == 4) {
          if (x(3).toUpperCase().equals("FAIL")) {
            rejectRateMap.put(VCM_INIT_FAIL_CNT, VCM_init_fail_cnt.getAndIncrement())
            rejectRateMap.put(SENSOR_THROW_TIGGER, "VCM_Init")
          }
        }
      }
      else if ("VCMInit1".toUpperCase().equals(foramtx2) && x(3) != null) {
        atypiaProcess.setVCM_init1(true)
        if (x.size == 4 && x(3).toUpperCase().equals("FAIL")) {
          if (atypiaProcess.getVCM_init) {
            // ?????????VCM_Init,VCM_Init1?????????VCM_Init2
            rejectRateMap.put(VCM_INIT2_FAIL_CNT, VCM_init1_fail_cnt.getAndIncrement())
            rejectRateMap.put(SENSOR_THROW_TIGGER, "VCM_Init2")
          } else {
            // ?????????VCM_Init,VCM_Init1?????????VCM_Init
            rejectRateMap.put(VCM_INIT_FAIL_CNT, VCM_init1_fail_cnt.getAndIncrement())
            rejectRateMap.put(SENSOR_THROW_TIGGER, "VCM_Init")
          }
        }
      }
      else if ("VCMInit2".toUpperCase().equals(foramtx2) && x(3) != null) {
        atypiaProcess.setVCM_init2(true)
        if (x.size == 4 && x(3).toUpperCase().equals("FAIL")) {
          if (atypiaProcess.getVCM_init && atypiaProcess.getVCM_init2) {
            // ?????????VCM_Init???VCM_Init1,VCM_Init2?????????VCM_Init3
            rejectRateMap.put(VCM_INIT3_FAIL_CNT, VCM_init2_fail_cnt.getAndIncrement())
            rejectRateMap.put(SENSOR_THROW_TIGGER, "VCM_Init3")
          } else if (atypiaProcess.getVCM_init || atypiaProcess.getVCM_init2) {
            // ?????????VCM_Init ???VCM_Init1 ????????????,VCM_Init2?????????VCM_Init2
            rejectRateMap.put(VCM_INIT2_FAIL_CNT, VCM_init2_fail_cnt.getAndIncrement())
            rejectRateMap.put(SENSOR_THROW_TIGGER, "VCM_Init2")
          } else {
            LOG.error("?????????????????????VCM_Init ???VCM_Init1 ????????????,?????????VCM_Init2" + resultReadFile)
          }
        }
      }
      else if ("VCMInit3".toUpperCase().equals(foramtx2) && x(3) != null) {
        if (x.size == 4) {
          if (x(3).toUpperCase().equals("FAIL")) {
            rejectRateMap.put(VCM_INIT3_FAIL_CNT, VCM_init3_fail_cnt.getAndIncrement())
            rejectRateMap.put(SENSOR_THROW_TIGGER, "VCM_Init3")
          }
          if ((!atypiaProcess.getVCM_init && !atypiaProcess.getVCM_init1) || (!atypiaProcess.getVCM_init && !atypiaProcess.getVCM_init2)
            || (!atypiaProcess.getVCM_init1 && !atypiaProcess.getVCM_init2)) {
            LOG.error("!atypiaProcess.getVCM_init && !atypiaProcess.getVCM_init1) || (!atypiaProcess.getVCM_init && !atypiaProcess.getVCM_init2) ||(!atypiaProcess.getVCM_init1 && !atypiaProcess.getVCM_init2) " + resultReadFile)

          }
        }
      } else if ("VCM_Poweron".toUpperCase().equals(foramtx2) && x(3) != null) {
        atypiaProcess.setVCM_PowerOn(true)
        if (x.size == 4 && x(3).toUpperCase().equals("FAIL")) {
          rejectRateMap.put(VCM_POWERON_FAIL_CNT, VCM_PowerOn_fail_cnt.getAndIncrement())
          rejectRateMap.put(SENSOR_THROW_TIGGER, "VCM_Poweron")
        }
      } else if ("VCMPoweron1".toUpperCase().equals(foramtx2) && x(3) != null) {
        atypiaProcess.setVCM_PowerOn1(true)
        if (x.size == 4 && x(3).toUpperCase().equals("FAIL")) {
          if (atypiaProcess.getVCM_PowerOn) {
            // ?????????VCM_Poweron,VCM_Poweron1?????????VCM_Poweron2
            rejectRateMap.put(VCM_POWERON2_FAIL_CNT, VCM_PowerOn1_fail_cnt.getAndIncrement())
            rejectRateMap.put(SENSOR_THROW_TIGGER, "VCM_Poweron2")
          } else {
            // ????????????VCM_Poweron,VCM_Poweron1?????????VCM_Poweron
            rejectRateMap.put(VCM_POWERON_FAIL_CNT, VCM_PowerOn1_fail_cnt.getAndIncrement())
            rejectRateMap.put(SENSOR_THROW_TIGGER, "VCM_Poweron")
          }
        }
      } else if ("VCMPoweron2".toUpperCase().equals(foramtx2) && x(3) != null) {
        atypiaProcess.setVCM_PowerOn2(true)
        if (x.size == 4 && x(3).toUpperCase().equals("FAIL")) {
          if (atypiaProcess.getVCM_PowerOn && atypiaProcess.getVCM_PowerOn1) {
            // ?????????VCM_Poweron???VCM_Poweron1,VCM_Poweron2?????????VCM_Poweron3
            rejectRateMap.put(VCM_POWERON3_FAIL_CNT, VCM_PowerOn2_fail_cnt.getAndIncrement())
            rejectRateMap.put(SENSOR_THROW_TIGGER, "VCM_Poweron3")
          } else if (atypiaProcess.getVCM_PowerOn || atypiaProcess.getVCM_PowerOn1) {
            // ?????????VCM_Poweron???VCM_Poweron1 ????????????,VCM_Poweron2?????????VCM_Poweron2
            rejectRateMap.put(VCM_POWERON2_FAIL_CNT, VCM_PowerOn2_fail_cnt.getAndIncrement())
            rejectRateMap.put(SENSOR_THROW_TIGGER, "VCM_Poweron2")
          } else {
            LOG.error("?????????????????????VCM_Poweron ??? VCM_Poweron1???????????? ,?????????VCM_Poweron2" + resultReadFile)
          }
        }
      }
      else if ("VCMPoweron3".toUpperCase().equals(foramtx2) && x(3) != null) {
        if (x.size == 4) {
          if (x(3).toUpperCase().equals("FAIL")) {
            rejectRateMap.put(VCM_POWERON3_FAIL_CNT, VCM_PowerOn3_fail_cnt.getAndIncrement())
            rejectRateMap.put(SENSOR_THROW_TIGGER, "VCM_Poweron3")
          }
          //?????????VCM_PowerOn2 ?????? (????????????VCM_PowerOn VCM_PowerOn1)
          if (!atypiaProcess.getVCM_PowerOn2 || (!atypiaProcess.getVCM_PowerOn && !atypiaProcess.getVCM_PowerOn1)) {
            LOG.error("?????????VCM_PowerOn2 || ?????????VCM_PowerOn VCM_PowerOn1" + resultReadFile)
          }
        }
      }
      else if (foramtx2.equals("ChartAlignment".toUpperCase()) && x(3) != null) {
        atypiaProcess.setAlignment(true)
      } else if (foramtx2.equals("ChartAlignment1".toUpperCase()) && x(3) != null) {
        atypiaProcess.setAlignment1(true)
        if (x.size == 4 && x(3).toUpperCase().equals("FAIL")) {
          //????????????ChartAlignment,??????ChartAlignment1?????????ChartAlignment2
          if (atypiaProcess.getAlignment) {
            rejectRateMap.put(CHART_ALIGNMENT2_FAIL_CNT, Chart_Alignment1_fail_cnt.getAndIncrement())
            rejectRateMap.put(SENSOR_THROW_TIGGER, "ChartAlignment2")
          }
        }
      } else if (foramtx2.equals("ChartAlignment2".toUpperCase()) && x(3) != null) {
        if (x.size == 4) {
          if (x(3).toUpperCase().equals("FAIL")) {
            rejectRateMap.put(CHART_ALIGNMENT2_FAIL_CNT, Chart_Alignment2_fail_cnt.getAndIncrement())
            rejectRateMap.put(SENSOR_THROW_TIGGER, "ChartAlignment2")
          }
          if (atypiaProcess.getAlignment && atypiaProcess.getAlignment1) {
            LOG.error("????????????ChartAlignment???ChartAlignment1???ChartAlignment2" + resultReadFile)
          } else if (!atypiaProcess.getAlignment && !atypiaProcess.getAlignment1) {
            LOG.error("ChartAlignment???ChartAlignment1?????????,????????????ChartAlignment2" + resultReadFile)

          }
        }
      } else if ("MTFcheck".toUpperCase().equals(foramtx2) && x(3) != null) {
        atypiaProcess.setMTF_Check(true)
      } else if ("MTFcheck1".toUpperCase().equals(foramtx2) && x(3) != null) {
        atypiaProcess.setMTF_Check1(true)
        if (x.size == 4 && x(3).toUpperCase().equals("FAIL") && atypiaProcess.getMTF_Check) {
          //????????????MTFcheck,??????MTFcheck1?????????MTFcheck2
          rejectRateMap.put(MTF_CHECK2_FAIL_CNT, MTF_Check1_fail_cnt.getAndIncrement())
          rejectRateMap.put(SENSOR_THROW_TIGGER, "MTF_Check2")
        }
      } else if ("MTFcheck2".toUpperCase().equals(foramtx2) && x(3) != null) {
        if (x.size == 4) {
          if (x(3).toUpperCase().equals("FAIL")) {
            rejectRateMap.put(MTF_CHECK2_FAIL_CNT, MTF_Check2_fail_cnt.getAndIncrement())
            rejectRateMap.put(SENSOR_THROW_TIGGER, "MTF_Check2")
          }
          if (atypiaProcess.getMTF_Check && atypiaProcess.getMTF_Check1) {
            LOG.error("????????????MTF_Check???MTF_Check1???MTF_Check2" + resultReadFile)
          } else if (!atypiaProcess.getMTF_Check && !atypiaProcess.getMTF_Check1) {
            // LOG.error("MTF_Check???MTF_Check1?????????,????????????MTF_Check2"+ resultReadFile)

          }
        }
      } else if ("VCMHall".toUpperCase().equals(foramtx2) && x(3) != null) {
        atypiaProcess.setVCM_Hall(true)
        if (x.size == 4) {
          if (x(3).toUpperCase().equals("FAIL")) {
            rejectRateMap.put(VCM_HALL_FAIL_CNT, VCM_Hall_fail_cnt.getAndIncrement())
            rejectRateMap.put(SENSOR_THROW_TIGGER, "VCM_Hall")
          }
        }
      } else if ("VCMHall1".toUpperCase().equals(foramtx2) && x(3) != null) {
        atypiaProcess.setVCM_Hall1(true)
        if (x.size == 4 && x(3).toUpperCase().equals("FAIL")) {
          if (atypiaProcess.getVCM_Hall) {
            //????????????VCMHall,??????VCMHall1?????????VCMHall2
            rejectRateMap.put(VCM_HALL2_FAIL_CNT, VCM_Hall1_fail_cnt.getAndIncrement())
            rejectRateMap.put(SENSOR_THROW_TIGGER, "VCM_Hall2")
          } else {
            rejectRateMap.put(VCM_HALL_FAIL_CNT, VCM_Hall1_fail_cnt.getAndIncrement())
            rejectRateMap.put(SENSOR_THROW_TIGGER, "VCM_Hall")
          }
        }
      } else if ("VCMHall2".toUpperCase().equals(foramtx2) && x(3) != null) {
        if (x.size == 4) {
          if (x(3).toUpperCase().equals("FAIL")) {
            rejectRateMap.put(VCM_HALL2_FAIL_CNT, VCM_Hall2_fail_cnt.getAndIncrement())
            rejectRateMap.put(SENSOR_THROW_TIGGER, "VCM_Hall2")
          }
          if (atypiaProcess.getVCM_Hall && atypiaProcess.getVCM_Hall1) {
            LOG.error("????????????VCM_Hall???VCM_Hall1???VCM_Hall2" + resultReadFile)
          } else if (!atypiaProcess.getVCM_Hall && !atypiaProcess.getVCM_Hall1) {
            LOG.error("VCM_Hall???VCM_Hall1?????????,????????????VCM_Hall2" + resultReadFile)
          }
        }
      } else if ("VCMMove".toUpperCase().equals(foramtx2) && x(3) != null) {
        atypiaProcess.setVCM_Move(true)
        if (x.size == 4) {
          if (x(3).toUpperCase().equals("FAIL")) {
            rejectRateMap.put(VCM_MOVE_FAIL_CNT, VCM_Move_fail_cnt.getAndIncrement())
            rejectRateMap.put(SENSOR_THROW_TIGGER, "VCM_Move")
          }
        }
      } else if ("VCMMove1".toUpperCase().equals(foramtx2) && x(3) != null) {
        atypiaProcess.setVCM_Move1(true)
        if (x.size == 4 && x(3).toUpperCase().equals("FAIL")) {
          if (atypiaProcess.getVCM_Move) {
            rejectRateMap.put(VCM_MOVE2_FAIL_CNT, VCM_Move1_fail_cnt.getAndIncrement())
            rejectRateMap.put(SENSOR_THROW_TIGGER, "VCM_Move2")
          } else {
            rejectRateMap.put(VCM_MOVE_FAIL_CNT, VCM_Move1_fail_cnt.getAndIncrement())
            rejectRateMap.put(SENSOR_THROW_TIGGER, "VCM_Move")
          }
        }
      } else if ("VCMMove2".toUpperCase().equals(foramtx2) && x(3) != null) {
        atypiaProcess.setVCM_Move2(true)
        if (x.size == 4) {
          if (x(3).toUpperCase().equals("FAIL")) {
            rejectRateMap.put(VCM_MOVE2_FAIL_CNT, VCM_Move2_fail_cnt.getAndIncrement())
            rejectRateMap.put(SENSOR_THROW_TIGGER, "VCM_Move2")
          }
          if (atypiaProcess.getVCM_Move && atypiaProcess.getVCM_Move1) {
            LOG.error("????????????VCM_Move???VCM_Move1???VCM_Move2" + resultReadFile)
          } else if (!atypiaProcess.getVCM_Move && !atypiaProcess.getVCM_Move1) {
            LOG.error("VCM_Move???VCM_Move1?????????,????????????VCM_Move2" + resultReadFile)
          }
        }
      }
      else if ("VCMOISInit".toUpperCase().equals(foramtx2) && x(3) != null) {
        atypiaProcess.setVCM_OIS_Init(true)
        if (x.size == 4) {
          if (x(3).toUpperCase().equals("FAIL")) {
            rejectRateMap.put(VCM_OIS_INIT_FAIL_CNT, VCM_OIS_Init_fail_cnt.getAndIncrement())
            rejectRateMap.put(SENSOR_THROW_TIGGER, "VCM_OIS_Init")
          }
        }
      } else if ("VCMOISInit1".toUpperCase().equals(foramtx2) && x(3) != null) {
        atypiaProcess.setVCM_OIS_Init1(true)
        if (x.size == 4 && x(3).toUpperCase().equals("FAIL")) {
          if (atypiaProcess.getVCM_OIS_Init) {
            rejectRateMap.put(VCM_OIS_INIT2_FAIL_CNT, VCM_OIS_Init1_fail_cnt.getAndIncrement())
            rejectRateMap.put(SENSOR_THROW_TIGGER, "VCM_OIS_Init2")
          } else {
            rejectRateMap.put(VCM_OIS_INIT_FAIL_CNT, VCM_OIS_Init1_fail_cnt.getAndIncrement())
            rejectRateMap.put(SENSOR_THROW_TIGGER, "VCM_OIS_Init")
          }
        }
      } else if ("VCMOISInit2".toUpperCase().equals(foramtx2) && x(3) != null) {
        if (x.size == 4) {
          if (x(3).toUpperCase().equals("FAIL")) {
            rejectRateMap.put(VCM_OIS_INIT2_FAIL_CNT, VCM_OIS_Init2_fail_cnt.getAndIncrement())
            rejectRateMap.put(SENSOR_THROW_TIGGER, "VCM_OIS_Init2")
          }
          if (atypiaProcess.getVCM_OIS_Init && atypiaProcess.getVCM_OIS_Init1) {
            LOG.error("????????????VCM_OIS_Init???VCM_OIS_Init2???VCM_OIS_Init2" + resultReadFile)
          } else if (!atypiaProcess.getVCM_OIS_Init && !atypiaProcess.getVCM_OIS_Init1) {
            LOG.error("VCM_OIS_Init???VVCM_OIS_Init1?????????,????????????VCM_OIS_Init2" + resultReadFile)
          }
        }
      }
      else if ("INITCheck".toUpperCase().equals(foramtx2) && x(3) != null) {
        if (x.size == 4) {
          if (x(3).toUpperCase().equals("FAIL")) {
            rejectRateMap.put(INIT_CHECK_FAIL_CNT, INIT_Check_fail_cnt.getAndIncrement())
            rejectRateMap.put(SENSOR_THROW_TIGGER, "INIT_Check")
          }
        }
      } else if ("LightpanelOC".toUpperCase().equals(foramtx2) && x(3) != null) {
        if (x.size == 4) {
          if (x(3).toUpperCase().equals("FAIL")) {
            rejectRateMap.put(LIGHTPANEL_OC_FAIL_CNT, Lightpanel_OC_fail_cnt.getAndIncrement())
            rejectRateMap.put(SENSOR_THROW_TIGGER, "Lightpanel_OC")
          }
        }
      }
      else if ("UVMTFCheck2".toUpperCase().equals(foramtx2) && x(3) != null) {
        if (x.size == 4) {
          if (x(3).toUpperCase().equals("FAIL")) {
            rejectRateMap.put(UV_MTF_CHECK2_FAIL_CNT, UV_MTF_Check2_fail_cnt.getAndIncrement())
            rejectRateMap.put(SENSOR_THROW_TIGGER, "UV_MTF_Check2")
          }
        }
      }
      else if ("SaveOC".toUpperCase().equals(foramtx2) && x(3) != null) {
        if (x.size == 4) {
          if (x(3).toUpperCase().equals("FAIL")) {
            rejectRateMap.put(SAVEOC_FAIL_CNT, SaveOC_fail_cnt.getAndIncrement())
            rejectRateMap.put(SENSOR_THROW_TIGGER, "Save_OC")
          }
        }
      } else if ("EpoxyInspection".toUpperCase().equals(foramtx2) && x(3) != null) {
        if (x.size == 4) {
          if (x(3).toUpperCase().equals("FAIL")) {
            rejectRateMap.put(EPOXY_INSPECTION_FAIL_CNT, Epoxy_Inspection_fail_cnt.getAndIncrement())
            rejectRateMap.put(SENSOR_THROW_TIGGER, "EpoxyInspection")
          }
        }
      } else if ("gripperopen".toUpperCase.equals(foramtx2) && x(3) != null) {
        if (x.size == 4) {
          // rejectRateMap.put("GripperOpen", x(3))
          if (x(3).toUpperCase().equals("PASS")) {
            rejectRateMap.put(GRIPPEROPEN_CNT, 1)
          }
        }
      }
    }
    //"Pre_Test".toUpperCase().equals(upperx1)
    else if ("Pre_Test".toUpperCase().equals(upperx1) && x.size > 3) {
      if ("PreTestStart".toUpperCase().equals(foramtx2) && x(3) != null) {
        rejectRateMap.put("Pre_Test_Start", x(3))
      }

    }

    rejectRateMap
  }


  def generateUpsertSQL(dataMap: JSONObject, area: String) = {

    var upsertSql: String = null
    if (dataMap != null && !dataMap.isEmpty) {
      var writeTable: String = null
      val keysBuf = new StringBuilder()
      val valuesBuf = new StringBuilder()

      if (area.startsWith("tai")) {
        writeTable = table_TH
      } else if (area.startsWith("cheng")) {
        writeTable = table_CB
      } else if (area.startsWith("gu")) {
        writeTable = table_GC
      }
      val inner = new Breaks
      for (k <- dataMap.keySet.toArray()) {
        var exist = false
        inner.breakable {
          for (rejectRateCol <- RejectRateModel.columnList.toArray()) {
            if (k.equals(rejectRateCol.toString.toUpperCase)) {
              keysBuf.append(k).append(",")
              valuesBuf.append(dataMap.get(k)).append(",")
              exist = true
              inner.break()
            }
          }
        }

        if (!exist) {
          keysBuf.append(k).append(",")
          valuesBuf.append("""'""").append(dataMap.get(k)).append("""'""").append(",")
        }
      }

      // ????????????????????????
      keysBuf.deleteCharAt(keysBuf.length - 1)
      valuesBuf.deleteCharAt(valuesBuf.length - 1)
      upsertSql = s"insert into $writeTable ($keysBuf) values ($valuesBuf)"

    }
    upsertSql
  }

  def generateCreateTableSQL(fields: util.List[String]) = {
    if (fields == null || fields.isEmpty) {

    }
    val createSqlList = new util.ArrayList[String]()

    //????????????
    val createsql_TH = new StringBuilder()
    val createsql_GC = new StringBuilder()
    val createsql_CB = new StringBuilder()

    createsql_TH.append(s"""create table IF NOT EXISTS $table_TH(ROWKEY varchar primary key,""")
    createsql_GC.append(s"""create table IF NOT EXISTS $table_GC(ROWKEY varchar primary key,""")
    createsql_CB.append(s"""create table IF NOT EXISTS $table_CB(ROWKEY varchar primary key,""")

    fields.remove("ROWKEY")
    // ?????? key value
    for (k <- fields.toArray()) {
      createsql_TH.append(s"""cf.$k varchar,""")
      createsql_GC.append(s"""cf.$k varchar,""")
      createsql_CB.append(s"""cf.$k varchar,""")
    }
    // ????????????????????????
    createsql_TH.deleteCharAt(createsql_TH.length - 1).append(")IMMUTABLE_ROWS = true, COMPRESSION='snappy' SPLIT ON ('0|','1|','2|','3|','4|','5|','6|','7|','8|')")
    createsql_GC.deleteCharAt(createsql_GC.length - 1).append(")IMMUTABLE_ROWS = true, COMPRESSION='snappy' SPLIT ON ('0|','1|','2|','3|','4|','5|','6|','7|','8|')")
    createsql_CB.deleteCharAt(createsql_CB.length - 1).append(")IMMUTABLE_ROWS = true, COMPRESSION='snappy' SPLIT ON ('0|','1|','2|','3|','4|','5|','6|','7|','8|')")
    createSqlList.add(createsql_TH.toString())
    createSqlList.add(createsql_GC.toString())
    createSqlList.add(createsql_CB.toString())

    // println("-----------------------------")
    // println(createsql_TH)
    createSqlList
  }

  /*  def generateAddColumnSQL(SQLcolSet: util.HashSet[String], originalcolSeq: util.List[String]) = {
      if (SQLcolSet == null || SQLcolSet.isEmpty || originalcolSeq == null || originalcolSeq.isEmpty) {
        LOG.error(".generateAddColumnSQL() ??????:SQLcolSet == null || SQLcolSet.isEmpty || originalcolSeq == null || originalcolSeq.isEmpty ??????!")
      }

      val notExistsColumn: util.ArrayList[String] = new util.ArrayList[String]()
      val THColBuf = new StringBuilder()
      THColBuf.append(s"ALTER TABLE $table_TH ADD if not exists ")
      val GCColBuf = new StringBuilder()
      GCColBuf.append(s"ALTER TABLE $table_GC ADD if not exists ")
      val CBColBuf = new StringBuilder()
      CBColBuf.append(s"ALTER TABLE $table_CB ADD if not exists ")

      var flag = false
      for (col <- SQLcolSet.toArray()) {

        val upperCol = col.toString.toUpperCase;
        if (!originalcolSeq.contains(upperCol)) {
          println("??????:" + upperCol + "-------------")
          THColBuf.append(upperCol).append(" varchar,")
          GCColBuf.append(upperCol).append(" varchar,")
          CBColBuf.append(upperCol).append(" varchar,")
          notExistsColumn.add(upperCol)
          flag = true

        }
      }

      var alterTbleModel: AlterTbleModel = new AlterTbleModel
      if (flag) {

        alterTbleModel.setCBsql(CBColBuf.deleteCharAt(CBColBuf.length - 1).toString())
        alterTbleModel.setGCsql(GCColBuf.deleteCharAt(GCColBuf.length - 1).toString())
        alterTbleModel.setTHsql(THColBuf.deleteCharAt(THColBuf.length - 1).toString())
        alterTbleModel.setNotExitsCloumn(notExistsColumn)

      }

      alterTbleModel
    }*/

}
