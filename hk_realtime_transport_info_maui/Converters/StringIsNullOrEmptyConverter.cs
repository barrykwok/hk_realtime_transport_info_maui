using System;
using System.Globalization;
using Microsoft.Maui.Controls;

namespace hk_realtime_transport_info_maui.Converters
{
    public class StringIsNullOrEmptyConverter : IValueConverter
    {
        public object Convert(object? value, Type targetType, object? parameter, CultureInfo culture)
        {
            if (value is string stringValue)
            {
                return string.IsNullOrWhiteSpace(stringValue);
            }
            
            return true; // If it's not a string, consider it empty
        }

        public object ConvertBack(object? value, Type targetType, object? parameter, CultureInfo culture)
        {
            // This is a one-way converter, so this method is not implemented
            throw new NotImplementedException();
        }
    }
} 