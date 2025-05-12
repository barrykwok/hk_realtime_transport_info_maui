using System.Globalization;
using System.Collections.Generic;
using Microsoft.Maui.Controls;

namespace hk_realtime_transport_info_maui.Resources
{
    public partial class StringResources : ResourceDictionary
    {
        // Dictionary to store translations
        private static Dictionary<string, Dictionary<string, string>> _localizedStrings;

        // Culture names
        private const string TraditionalChineseCulture = "zh-HK";
        private const string SimplifiedChineseCulture = "zh-CN";
        private const string DefaultCulture = "en-US";

        // Static constructor to initialize the translations dictionary
        static StringResources()
        {
            _localizedStrings = new Dictionary<string, Dictionary<string, string>>
            {
                // Traditional Chinese strings
                {
                    TraditionalChineseCulture, new Dictionary<string, string>
                    {
                        { "AppTitle", "路線資訊" },
                        { "AllRoutes", "所有路線" },
                        { "From", "從: {0}" },
                        { "To", "到: {0}" },
                        { "RouteDetails", "路線詳情" },
                        { "Route", "路線: {0}" },
                        { "Name", "名稱: {0}" },
                        { "Close", "關閉" },
                        { "NoRoutesFound", "未找到路線。" },
                        { "Refresh", "刷新" }
                    }
                },
                // Simplified Chinese strings
                {
                    SimplifiedChineseCulture, new Dictionary<string, string>
                    {
                        { "AppTitle", "路线信息" },
                        { "AllRoutes", "所有路线" },
                        { "From", "从: {0}" },
                        { "To", "到: {0}" },
                        { "RouteDetails", "路线详情" },
                        { "Route", "路线: {0}" },
                        { "Name", "名称: {0}" },
                        { "Close", "关闭" },
                        { "NoRoutesFound", "未找到路线。" },
                        { "Refresh", "刷新" }
                    }
                }
            };
        }

        // Constructor for the resource dictionary
        public StringResources()
        {
            // In a ResourceDictionary, the InitializeComponent() would typically be called here,
            // but since we've defined our own resources in code, we'll just apply the localization
            ApplyLocalization(CultureInfo.CurrentUICulture.Name);
        }

        // Get localized string based on the key and optional culture
        public static string GetLocalizedString(string key, string? cultureName = DefaultCulture)
        {
            // If no culture is specified, use the current UI culture
            cultureName ??= CultureInfo.CurrentUICulture.Name;
            
            // Check if we have translations for this culture
            if (_localizedStrings.TryGetValue(GetBaseCultureName(cultureName) ?? DefaultCulture, out var translations))
            {
                // Check if we have a translation for this key
                if (translations.TryGetValue(key, out var value))
                {
                    return value;
                }
            }
            
            // Return empty string if no translation is found
            // The default value in the XAML will be used
            return string.Empty;
        }
        
        // Apply localization to the resource dictionary
        public void ApplyLocalization(string cultureName)
        {
            string? baseCultureName = GetBaseCultureName(cultureName);
            
            // If we have translations for this culture
            if (baseCultureName != null && _localizedStrings.TryGetValue(baseCultureName, out var translations))
            {
                // Update all keys in the resource dictionary
                foreach (var key in translations.Keys)
                {
                    if (this.ContainsKey(key))
                    {
                        this[key] = translations[key];
                    }
                }
            }
        }
        
        // Helper method to get the base culture name
        private static string? GetBaseCultureName(string cultureName)
        {
            if (cultureName.StartsWith("zh-Hant") || cultureName == "zh-HK" || cultureName == "zh-TW")
            {
                return TraditionalChineseCulture;
            }
            else if (cultureName.StartsWith("zh"))
            {
                return SimplifiedChineseCulture;
            }
            
            // Default to English (no change to resource dictionary)
            return DefaultCulture;
        }
    }
} 