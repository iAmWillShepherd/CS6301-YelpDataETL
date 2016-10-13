using System.Collections.Generic;

namespace YelpDataETL
{
    public class Business
    {
        public string BusinessId { get; set; }
        public string Name { get; set; }
        public string FullAddress { get; set; }
        public string City { get; set; }
        public string State { get; set; }
        public float Latitude { get; set; }
        public float Longitude { get; set; }
        public float Stars { get; set; }
        public int ReviewCount { get; set; }
        public bool? Open { get; set; }
        public List<string> Categories { get; set; }
        public List<BusinessAttributeInfo> Attributes { get; set; }
        public Dictionary<string, KeyValuePair<string, string>> Hours { get; set; }

        public Business()
        {
            Categories = new List<string>();
            Attributes = new List<BusinessAttributeInfo>();
            Hours = new Dictionary<string, KeyValuePair<string, string>>();
        }
    }
}