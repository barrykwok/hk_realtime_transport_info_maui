using Microsoft.Maui.Controls;
using Microsoft.Maui.Hosting;

namespace hk_realtime_transport_info_maui
{
    public static class FontAwesomeRegister
    {
        public static MauiAppBuilder ConfigureFontAwesome(this MauiAppBuilder builder)
        {
            // Register Font Awesome fonts
            builder.ConfigureFonts(fonts =>
            {
                fonts.AddEmbeddedResourceFont(
                    typeof(FontAwesomeRegister).Assembly,
                    "Font.Awesome.fa-solid-900.ttf", 
                    "FontAwesomeSolid");
            });
            
            return builder;
        }
    }
} 