using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Web.Http;
using NadyApi.Models;
using System.Data;
using System.Data.SqlClient;

namespace NadyApi.Controllers
{
    public class ReportController : ApiController
    {
        KetNoi ketnoi = new KetNoi();

        DataClasses1DataContext db = new DataClasses1DataContext();
        [Route("api/report/getdsvung")]
        public IEnumerable<object> GetDSVung(DateTime tungay, DateTime denngay)
        {
            //var strSel = from q in db.sp_hh_hdhhgs
            //             where (q.ngayct >= tungay && q.ngayct <= denngay) && (q.huyhoadon == false) && (q.ma_kh != "") && (q.ma_kh.Contains('C'))
            //             join p in db.dm_kh_temps on q.ma_kh equals p.ma_kh
            //             group q by p.ma_kh into g
            //             select new { VM = g.Key, sotien = g.Sum(x => x.cong) };
            //var strSel = from q in db.sp_hh_hdhhgs
            //             where (q.ngayct >= tungay && q.ngayct <= denngay) && (q.huyhoadon == false) && (q.ma_kh != "") && (q.ma_kh.StartsWith("C"))
            //             group q by q.ma_kh into g
            //             select new { KH = g.Key, sotien = g.Sum(x => x.cong) };
            var strSel = from q in db.sp_hh_hdhhgs
                         where (q.ngayct >= tungay && q.ngayct <= denngay) && (q.huyhoadon == false) && (q.ma_kh != "") && (q.ma_kh.StartsWith("C"))
                         join p in db.dm_kh_temps on q.ma_kh equals p.ma_kh into pq
                         from p in pq.DefaultIfEmpty()
                         group q by p.VM into g
                         join d in db.dm_nhom_dts on g.Key equals d.ma_nhom 
                         select new{ VM=d.ten_nhom  , sotien=g.Sum(x=>x.cong )
                         };

            //string strSl = "select kh.VM , SUM(hd.cong) as sotien from dm_kh_temp as kh, sp_hh_hdhhg as hd where (ngayct between '2017-01-01' and '2017-01-31') and huyhoadon=0 and hd.ma_kh<>'' and hd.ma_kh like 'C%' and hd.ma_kh=kh.ma_kh group by kh.VM";
            //DataTable dt = ketnoi.getDataTable(strSl);
            return strSel.ToList<Object>() ;
            

        }
        [Route("api/report/getdstinh")]
        public IEnumerable<object> GetDSTinh(string VM,DateTime tungay, DateTime denngay)
        {

            var strSel = from q in db.sp_hh_hdhhgs
                         where (q.ngayct >= tungay && q.ngayct <= denngay) && (q.huyhoadon == false) && (q.ma_kh != "") && (q.ma_kh.StartsWith("C"))
                         join p in db.dm_kh_temps on new {q.ma_kh, VM} equals new {p.ma_kh,p.VM} into pq
                         from p in pq.DefaultIfEmpty()
                         group q by p.tinh into g
                         join d in db.dm_nhom_dts on g.Key equals d.ma_nhom
                         select new
                         {
                             VM = d.ten_nhom,
                             sotien = g.Sum(x => x.cong)
                         };
            return strSel.ToList<Object>();
        }
        [Route("api/report/getdsquan")]
        public DataTable  GetDSQuanHCM(DateTime tungay, DateTime denngay)
        {
            
            string strSel = string.Format("select kh.qh , SUM(hd.cong) as sotien from dm_kh_temp as kh, sp_hh_hdhhg as hd where (ngayct between '{0}' and '{1}') and huyhoadon=0 and hd.ma_kh<>'' and hd.ma_kh like 'C%' and hd.ma_kh=kh.ma_kh and kh.VM='HCM' group by kh.qh",tungay.ToString("yyyy-MM-dd"), denngay.ToString("yyyy-MM-dd"));
            DataTable dt = ketnoi.getDataTable(strSel);
            return dt;
        }
        [Route("api/report/getdsctv")]
        public DataTable GetDSCTV(DateTime tungay, DateTime denngay)
        {

            string strSel = string.Format("select hd.ma_ctv, ctv.ten_ctv, SUM(hd.cong) as sotien from sp_hh_hdhhg as hd, dm_ctv as ctv " +
                    "where (ngayct between '{0}' and '{1}') and huyhoadon=0 and hd.ma_kh<>'' " +
                    "and hd.ma_kh like 'C%'  and hd.ma_ctv !='' and hd.ma_ctv=ctv.ma_ctv " +
                    "group by hd.ma_ctv,ctv.ten_ctv " +
                    "order by sotien desc", tungay.ToString("yyyy-MM-dd"), denngay.ToString("yyyy-MM-dd"));
            DataTable dt = ketnoi.getDataTable(strSel);
            return dt;
        }
        [Route("api/report/getdsctvVM")]
        public DataTable GetDSCTVTheoVM(string vm, DateTime tungay, DateTime denngay)
        {

            string strSel = string.Format("select hd.ma_ctv, ctv.ten_ctv, SUM(hd.cong) as sotien from dm_kh_temp as kh, sp_hh_hdhhg as hd, dm_ctv as ctv "+
                    "where (ngayct between '{0}' and '{1}') and huyhoadon=0 and hd.ma_kh<>'' "+
                    "and hd.ma_kh like 'C%' and hd.ma_kh=kh.ma_kh and kh.VM='{2}' and hd.ma_ctv !='' and hd.ma_ctv=ctv.ma_ctv "+
                    "group by hd.ma_ctv,ctv.ten_ctv "+
                    "order by sotien desc", tungay.ToString("yyyy-MM-dd"), denngay.ToString("yyyy-MM-dd"),vm);
            DataTable dt = ketnoi.getDataTable(strSel);
            return dt;
        }
        [Route("api/report/getdsctvTinh")]
        public DataTable GetDSCTVTheoTinh(string tinh, DateTime tungay, DateTime denngay)
        {

            string strSel = string.Format("select hd.ma_ctv, ctv.ten_ctv, SUM(hd.cong) as sotien from dm_kh_temp as kh, sp_hh_hdhhg as hd, dm_ctv as ctv " +
                    "where (ngayct between '{0}' and '{1}') and huyhoadon=0 and hd.ma_kh<>'' " +
                    "and hd.ma_kh like 'C%' and hd.ma_kh=kh.ma_kh and kh.Tinh='{2}' and hd.ma_ctv !='' and hd.ma_ctv=ctv.ma_ctv " +
                    "group by hd.ma_ctv,ctv.ten_ctv " +
                    "order by sotien desc", tungay.ToString("yyyy-MM-dd"), denngay.ToString("yyyy-MM-dd"), tinh );
            DataTable dt = ketnoi.getDataTable(strSel);
            return dt;
        }
        [Route("api/report/getdsctvhcm")]
        public DataTable GetDSCTVHCM(DateTime tungay, DateTime denngay)
        {

            string strSel = string.Format("select hd.ma_ctv, ctv.ten_ctv, SUM(hd.cong) as sotien from dm_kh_temp as kh, sp_hh_hdhhg as hd, dm_ctv as ctv " +
                    "where (ngayct between '{0}' and '{1}') and huyhoadon=0 and hd.ma_kh<>'' " +
                    "and hd.ma_kh like 'C%' and hd.ma_kh=kh.ma_kh and kh.VM='HCM' and hd.ma_ctv !='' and hd.ma_ctv=ctv.ma_ctv " +
                    "group by hd.ma_ctv,ctv.ten_ctv " +
                    "order by sotien desc", tungay.ToString("yyyy-MM-dd"), denngay.ToString("yyyy-MM-dd"));
            DataTable dt = ketnoi.getDataTable(strSel);
            return dt;
        }
    }
}
