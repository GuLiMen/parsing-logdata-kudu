package cn.qtech.bigdata.model

import scala.annotation.meta.{beanGetter, beanSetter}
import scala.beans.BeanProperty


class AtypiaProcess {
  @BeanProperty var  MTF_Check:Boolean=_
  @BeanProperty var MTF_Check1:Boolean=_

  @BeanProperty var alignment:Boolean=_
  @BeanProperty var alignment1:Boolean=_

  @BeanProperty var VCM_init:Boolean=_
  @BeanProperty var VCM_init1:Boolean=_
  @BeanProperty var VCM_init2:Boolean=_

  @BeanProperty var VCM_PowerOn:Boolean=_
  @BeanProperty var VCM_PowerOn1:Boolean=_
  @BeanProperty var VCM_PowerOn2:Boolean=_

  @BeanProperty var VCM_Hall:Boolean=_
  @BeanProperty var VCM_Hall1:Boolean=_

  @BeanProperty var VCM_Move:Boolean=_
  @BeanProperty var VCM_Move1:Boolean=_
  @BeanProperty var VCM_Move2:Boolean=_

  @BeanProperty var VCM_OIS_Init:Boolean=_
  @BeanProperty var VCM_OIS_Init1:Boolean=_
}
