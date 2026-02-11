import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { AlertTriangle, CheckCircle2, AlertCircle } from "lucide-react";
import type { MemoryQualityMetrics } from "../types/api";

interface MemoryQualityCardProps {
  quality?: MemoryQualityMetrics;
}

/**
 * Get quality status display based on ratio
 */
function getQualityStatus(ratio: number) {
  if (ratio <= 0.1) {
    return {
      icon: CheckCircle2,
      text: "Excellent",
      className: "bg-green-500 hover:bg-green-600",
      textColor: "text-green-600",
    };
  } else if (ratio <= 0.2) {
    return {
      icon: CheckCircle2,
      text: "Good",
      className: "bg-blue-500 hover:bg-blue-600",
      textColor: "text-blue-600",
    };
  } else if (ratio <= 0.5) {
    return {
      icon: AlertCircle,
      text: "Fair",
      className: "bg-yellow-500 hover:bg-yellow-600",
      textColor: "text-yellow-600",
    };
  } else {
    return {
      icon: AlertTriangle,
      text: "Poor",
      className: "bg-red-500 hover:bg-red-600",
      textColor: "text-red-600",
    };
  }
}

/**
 * Format quality criteria key to human-readable label
 */
function formatCriteriaLabel(key: string): string {
  const labels: Record<string, string> = {
    missing_metadata: "Missing Metadata",
    empty_content: "Empty Content",
    no_embedding: "No Embedding",
    low_importance: "Low Importance",
  };
  return labels[key] || key;
}

export function MemoryQualityCard({ quality }: MemoryQualityCardProps) {
  if (!quality) {
    return (
      <Card>
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <AlertTriangle className="size-5" />
            Memory Quality
          </CardTitle>
          <CardDescription>Loading quality metrics...</CardDescription>
        </CardHeader>
      </Card>
    );
  }

  const qualityStatus = getQualityStatus(quality.low_quality_ratio);
  const StatusIcon = qualityStatus.icon;
  const percentage = (quality.low_quality_ratio * 100).toFixed(1);

  return (
    <Card>
      <CardHeader>
        <CardTitle className="flex items-center gap-2">
          <AlertTriangle className="size-5" />
          Memory Quality
        </CardTitle>
        <CardDescription>
          Quality analysis of {quality.total_memories.toLocaleString()} memories
        </CardDescription>
      </CardHeader>
      <CardContent>
        <div className="space-y-4">
          {/* Quality Overview */}
          <div className="flex items-center justify-between">
            <div>
              <p className="text-sm text-muted-foreground mb-1">
                Low Quality Ratio
              </p>
              <div className="flex items-baseline gap-2">
                <span className={`text-4xl font-bold ${qualityStatus.textColor}`}>
                  {percentage}%
                </span>
                <Badge className={qualityStatus.className}>
                  <StatusIcon className="size-3 mr-1" />
                  {qualityStatus.text}
                </Badge>
              </div>
              <p className="text-xs text-muted-foreground mt-1">
                {quality.low_quality_count} / {quality.total_memories} memories
              </p>
            </div>
          </div>

          {/* Quality Issues Distribution */}
          {quality.quality_criteria &&
            Object.keys(quality.quality_criteria).length > 0 && (
              <div className="pt-4 border-t">
                <h4 className="text-sm font-medium mb-3">Quality Issues</h4>
                <div className="space-y-2">
                  {Object.entries(quality.quality_criteria)
                    .filter(([_, count]) => count > 0)
                    .sort(([_, a], [__, b]) => b - a)
                    .map(([key, count]) => {
                      const maxCount = Math.max(
                        ...Object.values(quality.quality_criteria)
                      );
                      const widthPercent = maxCount > 0 ? (count / maxCount) * 100 : 0;

                      return (
                        <div key={key} className="space-y-1">
                          <div className="flex items-center justify-between text-sm">
                            <span className="text-muted-foreground">
                              {formatCriteriaLabel(key)}
                            </span>
                            <span className="font-medium">{count}</span>
                          </div>
                          <div className="h-2 bg-muted rounded-full overflow-hidden">
                            <div
                              className="h-full bg-primary rounded-full transition-all"
                              style={{ width: `${widthPercent}%` }}
                            />
                          </div>
                        </div>
                      );
                    })}
                </div>
              </div>
            )}

          {/* No Issues Message */}
          {quality.quality_criteria &&
            Object.values(quality.quality_criteria).every(
              (count) => count === 0
            ) && (
              <div className="pt-4 border-t">
                <div className="flex items-center gap-2 text-sm text-green-600">
                  <CheckCircle2 className="size-4" />
                  <span>No quality issues detected</span>
                </div>
              </div>
            )}
        </div>
      </CardContent>
    </Card>
  );
}
